package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-golang_kafka_1:9092",
		"delivery.timeout.ms": "0",
		// "acks":                "alls",
		"enable.idempotence": "true",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("test async", "test", producer, nil, deliveryChan)
	go DeliveryProduce(deliveryChan) // async
	// e := <-deliveryChain
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enivar")
	// } else {
	// 	fmt.Println("Message enviada:", msg.TopicPartition)
	// }

	producer.Flush(1000)
}

func DeliveryProduce(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
			}
		}
	}
}
