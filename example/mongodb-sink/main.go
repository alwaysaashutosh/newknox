// Package main
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/database"
)

type l1 struct {
	Value string `kmux:"value"`
	L2    l2     `kmux:"l2"`
}

type l2 struct {
	Value string `kmux:"value"`
	L3    l3     `kmux:"l3,omitempty"`
}

type l3 struct {
	Value int `kmux:"value,omitempty"`
}

func main() {
	// Init
	err := kmux.Init(&config.Options{
		LocalConfigFile: "kmux-config.yaml",
	})
	exitOnError(err)

	// Create new database sink
	ds, err := kmux.NewDatabaseSink()
	exitOnError(err)

	// Connect with the sink
	err = ds.Connect()
	exitOnError(err)

	// Sink.Upsert() API example
	upsertExample1(ds)
	upsertExample2(ds)

	// Disconnect the sink
	ds.Disconnect()
}

func upsertExample1(ds database.Sink) {
	// 1. Using Sink.Upsert(), you can push a struct directly into a collection in mongoDB.
	// 2. The struct fields can have `kmux` tag.
	// 3. The tag value represents the JSON key in the document.

	res := l1{"val34", l2{"val23", l3{204}}}

	err := ds.Upsert("test", database.Record{
		Row:   &res,
		Where: "value = ? and l2.value = ?",
		Args:  []any{res.Value, res.L2.Value},
	})
	exitOnError(err)
}

func upsertExample2(ds database.Sink) {
	// To dump arbitrary JSONs into MongoDB,
	// 1. Convert the json string to a nested map (`map[string]interface{}`)
	// 2. Pass the ref(map) to Sink.Upsert() as shown below

	jsonStr := `{
		"value": "val35",
		"l2": {
			"value": "val24",
			"l3": {
				"value": 205
			}
		}
	}`

	var res map[string]interface{}
	_ = json.Unmarshal([]byte(jsonStr), &res)

	err := ds.Upsert("test", database.Record{
		Row: &res,
	})
	exitOnError(err)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
