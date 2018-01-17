package main

import (
	"./oplogtailer"
	//"github.com/17media/api/experiment/carl/oplog-tailer/oplogtailer"

	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

var (
	mgourl = flag.String(
		"mongourl",
		"mongodb://aaa:bbb@localhost:59527/?authSource=admin",
		"The Mongo url to use for grab oplogs.",
	)
	esurl = flag.String(
		"esurl",
		"http://localhost:9527/mongo.oplogs",
		"The ElasticSearch url to use storing oplogs.",
	)
	sslEnabled  = flag.Bool("sslEnabled", false, "Which defines connected with SSL")
	env         = flag.String("env", "unknown", "Which environment logs come from")           //unknown
	label       = flag.String("label", "", "A tag to record where the last tailer is")        //empty
	filterRegex = flag.String("filterRegex", "", "Give prefix of namespace you want to tail") //"17media.*"
)

// SimpleLogger is an reference point
type SimpleLogger struct{}

// OnDelete called when the OpLogTailer receives an insert operation.
func (sl *SimpleLogger) OnDelete(event *oplogtailer.OpLoggerEvent) {
	log.Printf("Deleted %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, "Deleted", *env, *esurl)
}

// OnInsert called when the OpLogTailer receives an insert operation.
func (sl *SimpleLogger) OnInsert(event *oplogtailer.OpLoggerEvent) {
	log.Printf("Inserted %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, "Inserted", *env, *esurl)
}

// OnUpdate called when the OpLogTailer receives an insert operation.
func (sl *SimpleLogger) OnUpdate(event *oplogtailer.OpLoggerEvent) {
	log.Printf("Updated %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, "Updated", *env, *esurl)
}

// OnCmd called when the OpLogTailer receives an cmd operation.
func (sl *SimpleLogger) OnCmd(event *oplogtailer.OpLoggerEvent) {
	log.Printf("Cmd %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, "Cmd", *env, *esurl)
}

// OnNoop called when the OpLogTailer receives an no op operation.
func (sl *SimpleLogger) OnNoop(event *oplogtailer.OpLoggerEvent) {
	log.Printf("No Op %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, "No Op", *env, *esurl)
}

// OnUnknown called when the OpLogTailer receives an unknown operation.
func (sl *SimpleLogger) OnUnknown(event *oplogtailer.OpLoggerEvent) {
	log.Printf("Can't recognize %s %s %s %s\n", event.Ts, event.Ns, event.ID, event.Data)
	opc := fmt.Sprintf("Can't recognize: %s", event.Op)
	_ = esForwarding(event.Ts, event.Ns, event.ID, event.Data, opc, *env, *esurl)
}

func esForwarding(ts string, ns string, id string, data string, curd string, env string, esurl string) (err error) {
	v := map[string]string{"ts": ts, "ns": ns, "id": id, "data": data, "curd": curd, "env": env}
	jv, _ := json.Marshal(v)
	req, err := http.NewRequest("POST", esurl, bytes.NewBuffer(jv))
	req.Header.Set("X-Custom-Header", "no-cache")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func main() {
	flag.Parse()

	tailer := oplogtailer.NewOpLogTailer(mgourl, *sslEnabled, filterRegex, label, &SimpleLogger{})
	tailer.Start()
}
