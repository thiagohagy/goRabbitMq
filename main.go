package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

var amqpServerURL = "amqp://127.0.0.1" 
var topic string = "device_events"
var group string = "poc"

func main () {
	// start kafka
	var forever = make(chan bool)	

	connectRabbitMQ, err := amqp.Dial(amqpServerURL)
    if err != nil {
        panic(err)
    }
    defer connectRabbitMQ.Close()

	channelRabbitMQ, err := connectRabbitMQ.Channel()
    if err != nil {
        panic(err)
    }
    defer channelRabbitMQ.Close()

	_, err = channelRabbitMQ.QueueDeclare(
        "GolangRabbitMQTest", // queue name
        true,            // durable
        false,           // auto delete
        false,           // exclusive
        false,           // no wait
        nil,             // arguments
    )
    if err != nil {
        panic(err)
    }

	message := amqp.Publishing{
		ContentType: "text/plain",
		Body: []byte("Mensagem de teste"),
	}

	
	// producer	
	go func (){
		for{
			sendErr := channelRabbitMQ.Publish(
				"",              // exchange
				"GolangRabbitMQTest", // queue name
				false,           // mandatory
				false,           // immediate
				message,         // message to publish
			); 
		
			if sendErr != nil {
				fmt.Print(sendErr)
			} else {
				log.Printf("-->> Message sent with success")	 
			}
	
			time.Sleep(time.Second * 10)
			
		}
	}()


	// consumer
	messages, err := channelRabbitMQ.Consume(
        "GolangRabbitMQTest", // queue name
        "",              // consumer
        true,            // auto-ack
        false,           // exclusive
        false,           // no local
        false,           // no wait
        nil,             // arguments
    )
    if err != nil {
        log.Println(err)
    }

    // Make a channel to receive messages into infinite loop.
	go func() {
        for message := range messages {
            // For example, show received message in a console.
            log.Printf("<<-- Received message: %s\n", message.Body)
        }
    }()
	

	<-forever
	os.Exit(2)		
}
