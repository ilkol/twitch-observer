package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type UserCrossStats struct {
	VisitedChannels map[string]time.Time
}

type ChatEvent struct {
	Streamer  string    `json:"streamer"`
	Username  string    `json:"username"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	globalTracker = make(map[string]*UserCrossStats)
	trackerMutex  = sync.Mutex{}
)

const (
	CrossChannelLimit = 1
	TimeWindow        = 30 * time.Second
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

	go startTrackerCleaner()

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

		analyzeCrossChannelActivity(event)
	}
}

func analyzeCrossChannelActivity(event ChatEvent) {
	trackerMutex.Lock()
	defer trackerMutex.Unlock()

	stats, exists := globalTracker[event.Username]
	if !exists {
		globalTracker[event.Username] = &UserCrossStats{
			VisitedChannels: map[string]time.Time{
				event.Streamer: event.Timestamp,
			},
		}
		return
	}

	stats.VisitedChannels[event.Streamer] = event.Timestamp

	activeNowCount := 0
	var activeChannelsList []string

	for ch, lastTime := range stats.VisitedChannels {
		if time.Since(lastTime) <= TimeWindow {
			activeNowCount++
			activeChannelsList = append(activeChannelsList, ch)
		}
	}

	if activeNowCount > CrossChannelLimit {
		log.Printf("🚨 СЕТЕВАЯ АНОМАЛИЯ: Юзер [%s] ОДНОВРЕМЕННО пишет в чатах: %v за последние 30 сек! Подозрение на ботнет!\n",
			event.Username, activeChannelsList)
	}
}

func startTrackerCleaner() {
	for {
		time.Sleep(15 * time.Second)
		trackerMutex.Lock()
		now := time.Now()

		for user, stats := range globalTracker {
			for ch, lastTime := range stats.VisitedChannels {
				if now.Sub(lastTime) > TimeWindow {
					delete(stats.VisitedChannels, ch)
				}
			}
			if len(stats.VisitedChannels) == 0 {
				delete(globalTracker, user)
			}
		}
		trackerMutex.Unlock()
	}
}
