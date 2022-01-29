package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-golang_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	topics := []string{"test"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition, "Offset", msg.TopicPartition.Offset, "timestamp", msg.Timestamp)
		}
	}
}
