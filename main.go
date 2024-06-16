package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ProducerConfig struct {
	BootstrapServers string
}

func ProduceKafkaMessage(config ProducerConfig, topic string, object interface{}) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.BootstrapServers})

	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	objBytes, err := json.Marshal(object)

	if err != nil {
		panic(err)
	}

	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          objBytes,
	}, deliveryChan)

	if err != nil {
		panic(err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		panic(m.TopicPartition.Error)
	}

	p.Flush(15 * 1000)
}

type ConsumerConfig struct {
	BootstrapServers string
	GroupId          string
	AutoOffsetReset  string
}

func ConsumeKafkaMessage(callback func([]byte) error, topic string, config ConsumerConfig) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"group.id":          config.GroupId,
		"auto.offset.reset": config.AutoOffsetReset,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			err := callback(msg.Value)
			if err != nil {
				fmt.Println(err)
			}
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
