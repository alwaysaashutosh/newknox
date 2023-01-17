// Package main
package main

import (
	"encoding/json"
	"fmt"
	"os"

	kmux "github.com/ashutosh-the-beast/newknox"
	"github.com/ashutosh-the-beast/newknox/config"
	"github.com/ashutosh-the-beast/newknox/stream"
)

type book struct {
	Book   string `json:"book"`
	Author string `json:"author"`
}

var books = []book{
	{"Dsa", "Coreman"},
	{"Ramayan", "Valmiki"},
	{"KarmaYoga", "Swami VivekaNanda"},
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

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
