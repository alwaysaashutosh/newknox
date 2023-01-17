package stream

import (
	"context"
	"fmt"

	"github.com/accuknox/kmux/config"
	"github.com/rs/xid"
)

// Source interface describes the prototypes for kmux's source APIs for streams (Pulsar, Kafka or GRPC)
type Source interface {
	// Connect establishes connection with the source
	Connect() error

	// Next fetches next message from the source
	// The function call blocks until a new message is received.
	// Returns `io.EOF` if messages in the source are fully consumed.
	Next() ([]byte, error)

	// Disconnect terminates the connection with the source
	Disconnect()

	// Channel returns a channel associated with the source which can be used to receive
	// messages asynchronously.
	//
	// Channel internally spawns a goroutine, actively fetch messages from the source and
	// buffers them in the returned channel. Cancelling the context will close the returned
	// channel and stop the goroutine. Messages fetched from the source and buffered in
	// the returned channel before cancelling the context needs to be drained manually.
	Channel(context.Context) chan []byte
}

// NewSource returns a stream source driver based on kmux configuration
func NewSource(topic string) (Source, error) {
	if config.App.Source.StreamDriver == config.PulsarDriver {
		if config.Pulsar.Subscription == "" {
			subscription := fmt.Sprintf("kmux-sub-%s", xid.New().String())
			return NewPulsarSource(topic, subscription), nil
		}
		return NewPulsarSource(topic, config.Pulsar.Subscription), nil
	}
	return nil, fmt.Errorf("source driver %s not supported", config.App.Source.StreamDriver)
}
