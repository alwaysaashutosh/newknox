package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/accuknox/kmux/config"

	pb "github.com/accuknox/knox-gateway/pkg/grpc/knoxgateway/pb"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

var (
	mu     sync.Mutex
	conn   *grpc.ClientConn
	stream pb.KnoxGateway_PublishClient
	count  uint
)

// KnoxGatewaySink implements `stream.Sink` interface for AccuKnox GRPC gateway
type KnoxGatewaySink struct {
	topic string
}

// NewKnoxGatewaySink returns a stream sink for gRPC gateway.
func NewKnoxGatewaySink(Topic string) *KnoxGatewaySink {
	return &KnoxGatewaySink{
		topic: Topic,
	}
}

// Connect implements `Sink.Connect()`
func (kg *KnoxGatewaySink) Connect() (err error) {
	//locking the mutex
	mu.Lock()

	//Unlocking it after a single execution.
	defer mu.Unlock()

	// if count == 0 || conn == nil {
	// 	conn, err = grpc.Dial(config.KnoxGateway.Server, grpc.WithInsecure(), grpc.WithTimeout(5000))
	// 	if err != nil {
	// 		return fmt.Errorf("Failed to dial GRPC Server : Error - %s", err.Error())
	// 	}
	// 	if conn == nil {
	// 		return fmt.Errorf("Failed to dial GRPC Server : Error - %s", err.Error())
	// 	}

	fmt.Println("Connection Stablished ::::::: Mega Star Soorya")

	if count == 0 {
		conn, err = grpc.Dial(config.KnoxGateway.Server, grpc.WithInsecure())
		if err != nil {
			return fmt.Errorf("Failed to dial GRPC Server : Error - %s", err.Error())
		}
		log.Info().Msg("Established a new gRPC connection at =" + config.KnoxGateway.Server)

		client := pb.NewKnoxGatewayClient(conn)
		stream, err = client.Publish(context.Background())
		if err != nil {
			cerr := conn.Close()
			if cerr != nil {
				return fmt.Errorf("KnoxGatewaySink: Failed to close the connection , Failed to get client streeam . connectionerr - %s ,streamerr -%s  ", cerr, err)
			}
			return fmt.Errorf("KnoxGatewaySink: Failed to get client streeam. Error - %s", err)
		}
		log.Info().Msg("KnoxGatewayStream : Stream successfully created ")
	}
	if conn != nil && stream != nil {
		count = count + 1
	}

	return nil
}

// Flush implements `sink.Flush()`
func (kg *KnoxGatewaySink) Flush(data []byte) error {

	//creating a payload according to proto.
	Payload := pb.PubEvent{Topic: kg.topic, Data: data}

	//Checking wheather we have a stream configured or not .
	// if stream == nil || conn == nil {
	// 	//It will try to reconnect for 5 times .
	// 	for i := 0; i <= 5; i++ {
	// 		err := kg.Connect()
	// 		if err != nil {
	// 			time.Sleep(3 * time.Second)
	// 			continue
	// 		} else {
	// 			break
	// 		}
	// 	}
	// 	if conn == nil || stream == nil {
	// 		return fmt.Errorf("KnoxGatewaySink: Failed to send message. Uninitialized stream/Connection ")
	// 	}
	// }

	//Checking wheather we have a stream configured or not .
	if stream == nil {
		return fmt.Errorf("KnoxGatewaySink: Failed to send message. Uninitialized stream")
	}

	err := stream.Send(&Payload)
	if err != nil {
		return fmt.Errorf("KnoxGatewaySink: Failed to send message. Topic - %s, Message - %s, Error - %s", kg.topic, string(data), err)
	}
	var msg string
	if len(data) > 100 {
		msg = string(data[:100]) + "..."
	} else {
		msg = string(data)
	}
	log.Info().Msgf("KnoxGatewaySink: Topic - %s | Message - %s", kg.topic, msg)
	return nil
}

// Disconnect implements `Sink.Disconnect()`
func (kg *KnoxGatewaySink) Disconnect() {
	mu.Lock()
	defer mu.Unlock()

	//Checking wheather we have a stream configured or not .
	if stream == nil || conn == nil {
		log.Error().Msg("KnoxGatewaySink: Failed to Disconnect. Uninitialized stream")
	} else {
		count = count - 1
		if count == 0 {
			//Closing the stream/connection once there are no stream/conn left.
			_, _ = stream.CloseAndRecv()
			err := conn.Close()
			if err != nil {
				log.Error().Msg("KnoxGatewaySink: Failed to close the connection :" + err.Error())
			}
		}
	}
}

// ProcessChannel implements `Sink.ProcessChannel()`
func (kg *KnoxGatewaySink) ProcessChannel(ctx context.Context, events chan any, processFn SinkProcessFunc) {
	log.Info().Msg("KnoxGatewaySink: Process Channel is not Implemented yet")
}
