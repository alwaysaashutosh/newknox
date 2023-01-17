// Package stream implements the stream sink and source
package stream

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ashutosh-the-beast/newknox/config"
	"github.com/rs/zerolog/log"
)

// PulsarSink implements `stream.Sink` interface for Apache Pulsar
type PulsarSink struct {
	client   pulsar.Client
	options  pulsar.ClientOptions
	producer pulsar.Producer
	topic    string
	pubName  string
}

// NewPulsarSink returns a stream sink for Apache Pulsar
func NewPulsarSink(topic, publisher string) *PulsarSink {
	return &PulsarSink{
		options: config.Pulsar.Options,
		topic:   config.Pulsar.TopicPrefix + topic,
		pubName: publisher,
	}
}

// Connect implements `Sink.Connect()`
func (ps *PulsarSink) Connect() (err error) {
	ps.client, err = pulsar.NewClient(ps.options)
	if err != nil {
		return fmt.Errorf("PulsarSink: Failed to create pulsar client. %s", err)
	}

	ps.producer, err = ps.client.CreateProducer(pulsar.ProducerOptions{
		Topic:              ps.topic,
		Name:               ps.pubName,
		MaxPendingMessages: 1,
		BatchingMaxSize:    5242880, // 5MB
	})
	if err != nil {
		ps.client.Close()
		return fmt.Errorf("PulsarSink: Failed to create a producer for topic %s. %s", ps.topic, err)
	}

	return nil
}

// Flush implements `sink.Flush()`
func (ps *PulsarSink) Flush(data []byte) error {
	_, err := ps.producer.Send(context.Background(), &pulsar.ProducerMessage{Payload: data})
	if err != nil {
		return fmt.Errorf(
			"PulsarSink: Failed to send message. Topic - %s, Message - %s, Error - %s",
			ps.topic, string(data), err)
	}

	var msg string
	if len(data) > 100 {
		msg = string(data[:100]) + "..."
	} else {
		msg = string(data)
	}
	log.Info().Msgf("PulsarSink: Topic - %s | Message - %s", ps.topic, msg)
	return nil
}

// ProcessChannel implements `Sink.ProcessChannel()`
func (ps *PulsarSink) ProcessChannel(ctx context.Context, events chan any, processFn SinkProcessFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-events:
			if !ok {
				log.Info().Msgf("PulsarSink: Processed all the messages from sink channel")
				return
			}

			var bytes []byte
			var err error
			if processFn != nil {
				bytes, err = processFn(msg)
				if err != nil {
					log.Error().Msgf("PulsarSink: Failed to process message from sink channel. msg=%s, err=%s", msg, err)
					continue
				}
			} else {
				bytes, ok = msg.([]byte)
				if !ok {
					log.Error().Msgf("PulsarSink: Invalid data type sent through sink channel.")
					continue
				}
			}

			err = ps.Flush(bytes)
			if err != nil {
				log.Error().Msg(err.Error())
				continue
			}
		}
	}
}

// Disconnect implements `Sink.Disconnect()`
func (ps *PulsarSink) Disconnect() {
	ps.producer.Close()
	ps.client.Close()
}
