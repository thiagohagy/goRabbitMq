package kafka

import (
	"log"

	"github.com/Shopify/sarama"
)

type Server struct {
	DataCollector  sarama.SyncProducer
	Closed chan bool
	Topic string
	Group string
	Brokers []string
}

func (s *Server) Close() error { // close server function
	s.Closed <- true
	if err := s.DataCollector.Close(); err != nil {
		log.Println("Failed to shut down data collector cleanly", err)	
	}

	return nil
}

