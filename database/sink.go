package database

import (
	"fmt"

	"github.com/accuknox/kmux/config"
)

// Record defines a record that needs to be inserted or updated into the database
type Record struct {
	Row   any
	Where string
	Args  []any
}

// Sink interface for database
type Sink interface {
	// Connect establishes connection with the sink
	Connect() error

	// Upsert update/insert a record of a table/collection in the database.
	Upsert(table string, record Record) error

	// Disconnect terminates the connection with the sink
	Disconnect()
}

// NewSink returns a database sink driver based on kmux configuration
func NewSink() (Sink, error) {
	driver := config.App.Sink.DatabaseDriver
	if driver == config.MongoDBDriver {
		return NewMongoSink(), nil
	}
	return nil, fmt.Errorf("sink driver %s not supported", config.App.Sink.DatabaseDriver)
}
