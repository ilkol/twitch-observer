package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type ChatEvent struct {
	Streamer  string    `json:"streamer"`
	Username  string    `json:"username"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type UserStats struct {
	MsgCount   uint
	LastActive time.Time
}

var (
	userCache  = make(map[string]*UserStats)
	cacheMutex = sync.Mutex{}
)

const (
	SuspiciousLimit = 4
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9094"},
		Topic:    "twitch-chat-events",
		GroupID:  "analytics-group-v1",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	go startCacheCleaner()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Ошибка чтения из Kafka: %v\n", err)
			continue
		}

		var event ChatEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Printf("Ошибка десериализации JSON: %v\n", err)
			continue
		}

		checkSuspiciousActivity(event)
	}
}

func checkSuspiciousActivity(event ChatEvent) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	stats, exists := userCache[event.Username]
	if !exists {
		userCache[event.Username] = &UserStats{
			MsgCount:   1,
			LastActive: event.Timestamp,
		}
		return
	}

	if time.Since(stats.LastActive) > 10*time.Second {
		stats.MsgCount = 1
	} else {
		stats.MsgCount++
	}
	stats.LastActive = event.Timestamp

	if stats.MsgCount > SuspiciousLimit {
		log.Printf("⚠️ АНОМАЛИЯ: Пользователь [%s] флудит в чате %s! (%d сообщений за <10 сек). Возможный бот накрутки актива!\n",
			event.Username, event.Streamer, stats.MsgCount)
	}
}

func startCacheCleaner() {
	for {
		time.Sleep(30 * time.Second)
		cacheMutex.Lock()
		now := time.Now()
		for user, stats := range userCache {
			if now.Sub(stats.LastActive) > 1*time.Minute {
				delete(userCache, user)
			}
		}
		cacheMutex.Unlock()
	}
}
