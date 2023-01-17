// Package kmux provide source/sink connectors for different streaming tools and databases.
package kmux

import (
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/database"
	"github.com/accuknox/kmux/stream"
	"github.com/accuknox/kmux/vault"
)

// Init initializes kmux configuration settings
func Init(options *config.Options) error {
	return config.Init(options)
}

// NewStreamSink returns a stream sink based on kmux configuration
func NewStreamSink(topic string) (stream.Sink, error) {
	return stream.NewSink(topic)
}

// NewStreamSource returns a stream source based on kmux configuration
func NewStreamSource(topic string) (stream.Source, error) {
	return stream.NewSource(topic)
}

// NewDatabaseSink returns a database sink based on kmux configuration
func NewDatabaseSink() (database.Sink, error) {
	return database.NewSink()
}

// NewDatabaseSource returns a database source based on kmux configuration
func NewDatabaseSource() (database.Source, error) {
	return database.NewSource()
}

// NewVault returns a vault source based on kmux configuration
func NewVault(secretPath string) (vault.Vault, error) {
	return vault.NewVault(secretPath)
}
