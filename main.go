package main

import (
	"local/goKafka/consumer"
	"local/goKafka/kafka"
	"local/goKafka/producer"
	"log"
	"os"
)

var brokers = []string{"127.0.0.1:9092"} 
var topic string = "device_events"
var group string = "poc"

func main () {
	// start kafka
	var producerDone = make(chan bool)	
	var consumerDone = make(chan bool)	

	server := &kafka.Server{
		Topic: topic,
		Group: group,
		Brokers: brokers, 
	}
	
	producer := &producer.Producer{
		Server: *server,
		
	}		
	go producer.Produce(10, producerDone) 

	worker := &consumer.Worker{
		Server: *server,
	}
	go worker.Consume()

	defer func() { 
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	<-producerDone
	<-consumerDone
	os.Exit(2)		
}
