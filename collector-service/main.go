package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

type ChatEvent struct {
	Streamer  string    `json:"streamer"`
	Username  string    `json:"username"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	channels := []string{"5opka", "buster", "silvername", "bratishkinoff", "stray228"}

	targetChannel := "5opka"

	conn, err := kafka.DialLeader(context.Background(), "tcp", "127.0.0.1:9094", "twitch-chat-events", 0)
	if err != nil {
		log.Printf("Предупреждение автосоздания: %v (возможно топик уже есть)\n", err)
	} else {
		conn.Close()
		log.Println("Топик успешно инициализирован!")
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9094"),
		Topic:    "twitch-chat-events",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	u := url.URL{Scheme: "wss", Host: "irc-ws.chat.twitch.tv:443", Path: "/"}
	log.Printf("Подключение к IRC Twitch: %s...\n", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Ошибка подключения к сокетам: %v", err)
	}
	defer c.Close()
	_ = c.WriteMessage(websocket.TextMessage, []byte("PASS oauth:justinfan12345\r\n"))
	_ = c.WriteMessage(websocket.TextMessage, []byte("NICK justinfan12345\r\n"))

	for _, ch := range channels {
		_ = c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("JOIN #%s\r\n", ch)))
		log.Printf("Успешно зашли в чат канала: %s! Сбор логов запущен...\n", ch)
	}

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("Ошибка чтения сообщения из сокета: %v", err)
			return
		}
		rawStr := string(message)
		if strings.HasPrefix(rawStr, "PING") {
			_ = c.WriteMessage(websocket.TextMessage, []byte("PONG :tmi.twitch.tv\r\n"))
			continue
		}
		if strings.Contains(rawStr, "PRIVMSG") {
			parts := strings.SplitN(rawStr, " PRIVMSG ", 2)
			if len(parts) < 2 {
				continue
			}

			leftPart := parts[0] // ":username!username@username.tmi.twitch.tv"
			username := leftPart[1:strings.Index(leftPart, "!")]

			rightPart := parts[1]
			msgParts := strings.SplitN(rightPart, " :", 2)
			if len(msgParts) < 2 {
				continue
			}
			chatMessage := strings.TrimSpace(msgParts[1])

			event := ChatEvent{
				Streamer:  targetChannel,
				Username:  username,
				Message:   chatMessage,
				Timestamp: time.Now(),
			}

			payload, _ := json.Marshal(event)

			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(event.Username),
					Value: payload,
				},
			)
			if err != nil {
				log.Printf("Ошибка пуша в Кафку: %v\n", err)
			}
		} else {
			fmt.Println(rawStr)
		}
	}
}
