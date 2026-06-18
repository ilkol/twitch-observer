package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type ChatEvent struct {
	Streamer  string    `json:"streamer"`
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "127.0.0.1:9094", "twitch-chat-events", 0)
	if err != nil {
		log.Printf("Предупреждение автосоздания: %v (возможно топик уже есть)\n", err)
	} else {
		conn.Close() // Закрываем тестовое соединение, оно сделало свою работу
		log.Println("Топик успешно инициализирован!")
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9094"),
		Topic:    "twitch-chat-events",
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	log.Println("Collector запущен.")

	for {
		event := ChatEvent{
			Streamer:  "il_kol",
			Username:  "il_kol",
			Timestamp: time.Now(),
		}

		payload, _ := json.Marshal(event)

		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.Username),
			Value: payload,
		})
		if err != nil {
			log.Printf("Ошибка отправки в Kafka %v\n", err)
		} else {
			log.Println("Сообщение успешно отправлено в Kafka")
		}

		time.Sleep(2 * time.Second)
	}
}
