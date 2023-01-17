package stream

import (
	"context"
	"io"

	"github.com/accuknox/kmux/config"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rs/zerolog/log"
)

const (
	queueSize = 1024
)

// PulsarSource implements `stream.Source` interface for Apache Pulsar
type PulsarSource struct {
	client       pulsar.Client
	options      pulsar.ClientOptions
	consumer     pulsar.Consumer
	topic        string
	subscription string
	receiver     chan pulsar.ConsumerMessage
}

// NewPulsarSource returns a stream source for Apache Pulsar
func NewPulsarSource(topic, subscription string) *PulsarSource {
	return &PulsarSource{
		options:      config.Pulsar.Options,
		topic:        config.Pulsar.TopicPrefix + topic,
		subscription: subscription,
	}
}

// Connect implements `Source.Connect()`
func (ps *PulsarSource) Connect() (err error) {
	ps.client, err = pulsar.NewClient(ps.options)
	if err != nil {
		log.Error().Msgf("Failed to create pulsar client. %s", err)
		return err
	}

	ps.receiver = make(chan pulsar.ConsumerMessage, queueSize)

	ps.consumer, err = ps.client.Subscribe(pulsar.ConsumerOptions{
		Topics:                      []string{ps.topic},
		SubscriptionName:            ps.subscription,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		MessageChannel:              ps.receiver,
	})
	if err != nil {
		log.Error().Msgf("Failed to subscribe topics. %s", err)
		ps.client.Close()
		return err
	}

	return nil
}

// Channel implements `Source.Channel()`
func (ps *PulsarSource) Channel(ctx context.Context) chan []byte {
	chanAny := make(chan []byte, queueSize)
	go func() {
		defer close(chanAny)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := ps.Next()
				if err == io.EOF {
					return
				}
				chanAny <- msg
			}
		}
	}()
	return chanAny
}

// Next implements `Source.Next()`
func (ps *PulsarSource) Next() ([]byte, error) {
	msg, ok := <-ps.receiver
	if !ok {
		return nil, io.EOF
	}
	_ = msg.Consumer.Ack(msg)
	return msg.Payload(), nil
}

// Disconnect implements `Source.Disconnect()`
func (ps *PulsarSource) Disconnect() {
	ps.consumer.Close()
	ps.client.Close()
}
