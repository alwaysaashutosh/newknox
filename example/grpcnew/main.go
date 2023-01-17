package main

import (
	"context"
	"encoding/json"

	"github.com/accuknox/knox-gateway/pkg/grpc/knoxgateway/pb"
	"github.com/ashutosh-the-beast/newknox/config"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("34.136.230.72:3000", grpc.WithInsecure())
	// conn, err := grpc.Dial("knox-gateway-dev.accuknox.com:3000", grpc.WithInsecure())
	if err != nil {
		log.Print(err)
		// return fmt.Errorf("Failed to dial GRPC Server : Error - %s", err.Error())
	}
	log.Info().Msg("Established a new gRPC connection at =" + config.KnoxGateway.Server)

	client := pb.NewKnoxGatewayClient(conn)
	stream, err := client.Publish(context.Background())
	if err != nil {
		log.Print(err)
		// cerr := conn.Close()
		// if cerr != nil {
		// 	return fmt.Errorf("KnoxGatewaySink: Failed to close the connection , Failed to get client streeam . connectionerr - %s ,streamerr -%s  ", cerr, err)
		// }
		// return fmt.Errorf("KnoxGatewaySink: Failed to get client streeam. Error - %s", err)
	}
	log.Info().Msg("KnoxGatewayStream : Stream successfully created ")
	d1 := "hello go"
	data, _ := json.Marshal(&d1)
	Payload := pb.PubEvent{Topic: "testing", Data: data}

	//Checking wheather we have a stream configured or not .
	// if stream == nil {
	// 	log.Print("Nil error")
	// 	// return fmt.Errorf("KnoxGatewaySink: Failed to send message. Uninitialized stream")
	// }

	err = stream.Send(&Payload)
	if err != nil {
		log.Print(err)
		// return fmt.Errorf("KnoxGatewaySink: Failed to send message. Topic - %s, Message - %s, Error - %s", kg.topic, string(data), err)
	} else {
		log.Print("Messsage Dropped Successfully")
	}

	// var msg string
	// if len(data) > 100 {
	// 	msg = string(data[:100]) + "..."
	// } else {
	// 	msg = string(data)
	// }
	// log.Info().Msgf("KnoxGatewaySink: Topic - %s | Message - %s", kg.topic, msg)
	// return nil

}
