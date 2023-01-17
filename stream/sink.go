package stream

import (
	"context"
	"fmt"

	"github.com/accuknox/kmux/config"
	"github.com/rs/xid"
)

// SinkProcessFunc describes the prototype for functions that can be passed to Sink.ProcessChannel()
type SinkProcessFunc func(any) ([]byte, error)

// Sink interface describes the prototypes for kmux's sink APIs for streams (Pulsar, Kafka or GRPC)
type Sink interface {
	// Connect establishes connection with the sink
	Connect() error

	// Flush sends []byte through the sink. The function call
	// blocks until the data is sent to the sink endpoint.
	Flush([]byte) error

	// Disconnect terminates the connection with the sink
	Disconnect()

	// ProcessChannel actively fetch messages from the channel and calls
	// SinkProcessFunc on each message. The data returned from SinkProcessFunc
	// will be flushed through the sink.
	//
	// By default, this is a blocking function which only returns when the channel
	// is closed or an error occurred. This function can be prefixed with `go` and
	// executed as a separate goroutine for asynchronous processing of messages.
	// In such cases, cancelling the context will stop the processing, free its resource
	// and exit from the goroutine.
	ProcessChannel(context.Context, chan any, SinkProcessFunc)
}

// NewSink returns a stream sink driver based on kmux configuration
func NewSink(topic string) (Sink, error) {
	if config.App.Sink.StreamDriver == config.PulsarDriver {
		publisher := fmt.Sprintf("kmux-pub-%s", xid.New().String())
		return NewPulsarSink(topic, publisher), nil
	} else if config.App.Sink.StreamDriver == config.KnoxGatewayDriver {
		return NewKnoxGatewaySink(topic), nil
	}
	return nil, fmt.Errorf("sink driver %s not supported", config.App.Sink.StreamDriver)
}
