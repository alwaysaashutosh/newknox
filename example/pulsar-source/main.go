// Package main
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/stream"
)

func main() {
	// Init
	err := kmux.Init(&config.Options{
		LocalConfigFile: "kmux-config.yaml",
	})
	exitOnError(err)

	// Create new stream source
	ss, err := kmux.NewStreamSource("book")
	exitOnError(err)

	// Connect with the source
	err = ss.Connect()
	exitOnError(err)

	// Source.Next() API example
	next(ss)

	// Source.Channel() API example
	channel(ss)

	// Disconnect from the source
	ss.Disconnect()
}

func next(ss stream.Source) {

	// Consume 5 messages from Pulsar

	for i := 0; i < 5; i++ {

		// Source.Next()
		data, err := ss.Next()
		exitOnError(err)

		fmt.Println("New message received:", string(data))
	}
}

func channel(ss stream.Source) {

	// Consume messages using a channel

	// Source.Channel()
	msgChannel := ss.Channel(context.Background())

	for data := range msgChannel {
		fmt.Println("New message received:", string(data))
	}
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
