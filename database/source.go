package database

import (
	"fmt"

	"github.com/accuknox/kmux/config"
)

// Query contains generic SQL-like query parameters
type Query struct {
	Rows    any
	Where   string
	Args    []any
	GroupBy []string
	Limit   int64
	Offset  int64
	OrderBy []OrderByColumn
}

// OrderByColumn defines <column_name, sorting_order> pair
type OrderByColumn struct {
	// Column Name
	Name string

	// `Desc` should be true if the sorting order is Descending.
	Desc bool
}

// Source interface for database
type Source interface {
	// Connect establishes connection with the source
	Connect() error

	// Get record(s) from a table/collection in the database.
	Get(table string, query Query) error

	// Returns the total count of records matching the query.
	Count(table string, query Query) (int32, error)

	// Disconnect terminates the connection with the source
	Disconnect()
}

// NewSource returns a database source driver based on kmux configuration
func NewSource() (Source, error) {
	driver := config.App.Source.DatabaseDriver
	if driver == config.MongoDBDriver {
		return NewMongoSource(), nil
	}
	return nil, fmt.Errorf("source driver %s not supported", driver)
}
