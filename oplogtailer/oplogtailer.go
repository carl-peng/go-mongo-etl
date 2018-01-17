package oplogtailer

import (
	"crypto/tls"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net"
	"time"
)

// OpLoggerEvent is result set from oplog tailer
type OpLoggerEvent struct {
	Ts   string //bson.MongoTimestamp
	ID   string //bson.M
	Data string //bson.M
	Ns   string
	Op   string
}

// OpLogger is the interface that must be implemented and registered with OpLogTailer
// class so that the events can be dispatched to it.
type OpLogger interface {
	// Method called when the OpLogTailer receives a deletion operation.
	OnDelete(event *OpLoggerEvent)

	// Method called when the OpLogTailer receives an update operation.
	OnUpdate(event *OpLoggerEvent)

	// Method called when the OpLogTailer receives an insert operation.
	OnInsert(event *OpLoggerEvent)

	// Method called when the OpLogTailer receives an command operation.
	OnCmd(event *OpLoggerEvent)

	// Method called when the OpLogTailer receives an specied "non-op" operation.
	OnNoop(event *OpLoggerEvent)

	// Method called when the OpLogTailer can not recognize received operation.
	OnUnknown(event *OpLoggerEvent)
}

// OpLogTailer is the tailer opject which contains mongo session and cursor
type OpLogTailer struct {
	collectionToTail *string
	session          *mgo.Session
	opLogger         OpLogger
}

// Internal collection maintained by the oplog tailer to keep track of information, like
// the timestamp from which it should start reading on restart.
type opLogTailerInfo struct {
	FilterRegex          string              "filterRegex"
	StartReadingFromTime bson.MongoTimestamp `bson:"startReadingFromTime"`
	Label                string              `bson:"label"`
}

// ObjectID is the property to describe object id
type ObjectID struct {
	ID string `bson:"_id"`
}

type opLogEntry struct {
	Ts bson.MongoTimestamp `bson:"ts"` //Timestamp
	H  int64               `bson:"h"`  //HistoryID
	V  int                 `bson:"v"`  //MongoVersion
	Op string              `bson:"op"` //Operation
	Ns string              `bson:"ns"` //Namespace
	O  bson.M              `bson:"o"`  //Object
	O2 bson.M              `bson:"o2"` //QueryObject
}

var tailerInfoCollection *mgo.Collection
var session *mgo.Session
var tailerInfo *opLogTailerInfo

const (
	mgoAuthDb  = "admin"
	mgoTimeout = 0 * time.Second
)

// NewOpLogTailer creates a new OpLogTailer.
func NewOpLogTailer(url *string, ssl bool, filterRegex *string, label *string, opLogger OpLogger) *OpLogTailer {
	var err error

	log.Printf("SSL: %#v", ssl)
	mi, _ := mgo.ParseURL(*url)
	mi.Database = mgoAuthDb
	mi.Timeout = mgoTimeout

	// Check if SSL is enabled
	if ssl {
		tlsConfig := &tls.Config{}
		tlsConfig.InsecureSkipVerify = true
		mi.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	session, err = mgo.DialWithInfo(mi)
	session.SetMode(mgo.Strong, true)
	if err != nil {
		log.Fatalf("Can't open session with %s: %s", url, err)
	}

	// Create/open the mongotailer.oplogtailerinfo namespace.
	tailerInfoCollection = session.DB("mongotailer").C("oplogtailerinfo")

	tailerInfo = &opLogTailerInfo{
		FilterRegex:          *filterRegex,
		StartReadingFromTime: bson.MongoTimestamp(time.Now().Unix() << 32),
		Label:                *label}

	// Read the oplogtailerinfo for the collection to be tailed.
	query := tailerInfoCollection.Find(buildOpLogTailerInfoSelector())
	var count int
	count, err = query.Count()
	if err != nil {
		log.Fatalf("Error getting count for oplogtailerinfo: %+v", err)
	}

	if count > 1 {
		log.Fatalf(
			`"The collection oplogtailerinfo has more than one document for tailed collection [%s] and label [%s].\n  
			There should only be one document per tailed collection and label.  Pleaes correct and restart."`,
			tailerInfo.FilterRegex,
			tailerInfo.Label,
		)
	}

	if count == 0 {

		// There isn't a document yet for collection and label, so create on.
		log.Printf("Creating %+v", tailerInfo)
		tailerInfoCollection.Insert(tailerInfo)
	} else {

		// We've eliminated the count > 1 and count == 0.
		// This must mean there is exactly 1, so read it in.

		query.One(&tailerInfo)
		log.Printf("Read %+v", tailerInfo)
	}

	return &OpLogTailer{
		session:  nil,
		opLogger: opLogger}
}

func buildOpLogTailerInfoSelector() bson.M {
	var andClause [2]bson.M

	andClause[0] = bson.M{"filterRegex": tailerInfo.FilterRegex}
	andClause[1] = bson.M{"label": tailerInfo.Label}

	opLogTailerInfoSelector := bson.M{"$and": andClause}
	log.Printf("oplogtailerinfoselector  = %+v", opLogTailerInfoSelector)
	return opLogTailerInfoSelector
}

func buildOpLogSelector() bson.M {
	var andClause [2]bson.M

	andClause[0] = bson.M{"ts": bson.M{"$gt": tailerInfo.StartReadingFromTime}}
	andClause[1] = bson.M{"ns": bson.M{"$regex": bson.RegEx{tailerInfo.FilterRegex, ""}}}

	opLogSelector := bson.M{"$and": andClause}
	log.Printf("oplogselector = %+v", opLogSelector)
	return opLogSelector
}

// Start establishes connection and borrow tailable cursors
func (olt *OpLogTailer) Start() error {
	collection := session.DB("local").C("oplog.rs")
	log.Printf("coll = %+v", collection)

	iter := collection.Find(buildOpLogSelector()).LogReplay().Sort("$natural").Tail(-1)

	//create a full channel
	controlCh := make(chan int, 100)
	for i := 0; i < 100; i++ {
		controlCh <- 1
	}

	var result opLogEntry
	for {
		for iter.Next(&result) {
			//log.Printf("result = %+v", result)
			_ = <-controlCh
			go func() {
				defer func() {
					controlCh <- 0
				}()

				tailerInfo.StartReadingFromTime = result.Ts
				tailerInfoCollection.Update(
					bson.M{"filterRegex": tailerInfo.FilterRegex, "label": tailerInfo.Label},
					tailerInfo,
				)
			}()

			_ = <-controlCh
			go func(result opLogEntry) {
				defer func() {
					controlCh <- 0
				}()

				event := &OpLoggerEvent{
					Ts:   fmt.Sprintf("%+v", (result.Ts >> 32)),
					ID:   fmt.Sprintf("%s", result.O2),
					Data: fmt.Sprintf("%+v", result.O),
					Ns:   fmt.Sprintf("%s", result.Ns),
					Op:   fmt.Sprintf("%s", result.Op),
				}
				switch result.Op {
				case "u":
					olt.opLogger.OnUpdate(event)
					return
				case "i":
					olt.opLogger.OnInsert(event)
					return
				case "d":
					olt.opLogger.OnDelete(event)
					return
				case "c":
					olt.opLogger.OnCmd(event)
					return
				case "n":
					olt.opLogger.OnNoop(event)
					return
				default:
					olt.opLogger.OnUnknown(event)
				}
			}(result)
		}

		if iter.Err() != nil {
			log.Printf("Got error: %+v", iter.Err())
			return iter.Close()
		}

		if iter.Timeout() {
			continue
		}

		// If we are here, it means something other than a timeout occurred, so let's
		// try and restart the tailing cursor.
		query := collection.Find(buildOpLogSelector())
		iter = query.Sort("$natural").Tail(5 * time.Second)
	}

	//block until all child thread completed
	for i := 0; i < 100; i++ {
		_ = <-controlCh
	}

	return iter.Close()
}
