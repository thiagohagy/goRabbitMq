package producer

import (
	"local/goKafka/kafka"
	"log"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

type Producer struct {
	Server kafka.Server
}

// STRUCT  RELATED FUNCS 

func (p *Producer) init()  {
	p.Server.DataCollector = NewSyncProducer(p.Server.Brokers)
}

func (p *Producer) Produce(limit int, done chan bool)  {
	p.init()
	var count int
	for  count < limit {

		p.CreateEvent()
				
		if limit != 0 {
			count+=1
		}

		wait := rand.Intn(5 - 2) + 2
		time.Sleep(time.Second * time.Duration(wait))		
		// fmt.Println("\r\n...Waiting for ", wait ," seconds to send next... \r\n")		
	}

	done <- true

}


func (p *Producer) CreateEvent() {
	
	message := sarama.ProducerMessage{
		Topic: p.Server.Topic,
		Value: sarama.StringEncoder("Event"),
	}

	_, _, err := p.Server.DataCollector.SendMessage(&message)

	if err != nil {
		panic("Error sending message")
	} 
	
	log.Printf("-->> Message sent to ",p.Server.Topic  )	
	
	return
}

// NOT STRUCT  RELATED FUNCS 

func  NewSyncProducer(brokers []string)  sarama.SyncProducer { 
	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

