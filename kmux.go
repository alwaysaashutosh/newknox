// Package kmux provide source/sink connectors for different streaming tools and databases.
package kmux

import (
	"github.com/ashutosh-the-beast/newknox/config"

	"github.com/ashutosh-the-beast/newknox/stream"
)

// Init initializes kmux configuration settings
func Init(options *config.Options) error {
	return config.Init(options)
}

// NewStreamSink returns a stream sink based on kmux configuration
func NewStreamSink(topic string) (stream.Sink, error) {
	return stream.NewSink(topic)
}
