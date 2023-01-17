// Package main
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/stream"
)

type book struct {
	Book   string `json:"book"`
	Author string `json:"author"`
}

var books = []book{
	{"Thirukkural", "Thiruvalluvar"},
	{"Silappathikaram", "Ilango Adigal"},
	{"Manimekalai", "Sithalai Sattanar"},
	{"Sivaka Sintamani", "Thirutakkathevar"},
	{"Kundalakesi", "Nathakuthanaar"},
}

func main() {
	// Init
	err := kmux.Init(&config.Options{
		LocalConfigFile: "kmux-config.yaml",
	})
	exitOnError(err)

	// Create a stream sink
	ss, err := kmux.NewStreamSink("book")
	exitOnError(err)

	// Connect with the sink
	err = ss.Connect()
	exitOnError(err)

	// Sink.Flush() API example
	flush(ss)

	// Sink.ProcessChannel() API example
	processChannel(ss)

	// Disconnect from the sink
	ss.Disconnect()
}

func flush(ss stream.Sink) {

	// Publish []books to Pulsar synchronously

	for _, book := range books {
		var bytes []byte

		book := book // prevent memory aliasing issue since address of the for loop var is used in next stmt
		bytes, err := json.Marshal(&book)
		exitOnError(err)

		// Sink.Flush()
		err = ss.Flush(bytes)
		exitOnError(err)
	}
}

func processChannel(ss stream.Sink) {

	// Publish []books to Pulsar asynchronously

	channel := make(chan any, 5)
	for _, book := range books {
		channel <- book
	}
	close(channel)

	ctx, sinkCtxCancel := context.WithCancel(context.Background())

	// Sink.ProcessChannel()
	go ss.ProcessChannel(ctx, channel, func(data interface{}) ([]byte, error) {
		book := data.(book)
		return json.Marshal(&book)
	})

	// When the sink process job is no longer needed, cancel the context
	sinkCtxCancel()
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
