package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"chat_core/internal/broker"
	"chat_core/internal/domain"
	"chat_core/internal/repository"

	"github.com/google/uuid"
)

type Worker struct {
	repo     repository.OutboxRepository
	chatRepo *repository.ChatRepository
	broker   *broker.RabbitMQClient
	stop     chan struct{}
}

func NewWorker(repo repository.OutboxRepository, chatRepo *repository.ChatRepository, broker *broker.RabbitMQClient) *Worker {
	return &Worker{
		repo:     repo,
		chatRepo: chatRepo,
		broker:   broker,
		stop:     make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stop)
}

func (w *Worker) processBatch(ctx context.Context) {
	// Start Transaction
	tx, err := w.repo.BeginTx(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback() // Rollback if not committed

	events, err := w.repo.FetchPending(ctx, tx, 50) // Batch size 50
	if err != nil {
		log.Printf("Failed to fetch pending events: %v", err)
		return
	}

	if len(events) == 0 {
		return
	}

	var processedIDs []uuid.UUID
	for _, event := range events {
		// Unmarshal payload to ensure it's valid JSON before publishing,
		// although we store it as JSONB so it should be fine.
		// We publish the whole event structure so the consumer knows the event type.

		// We need to unmarshal the payload to interface{} to pass to Publish,
		// or we can just pass the raw message if we change Publish signature.
		// But Publish takes interface{} and marshals it.
		// Let's unmarshal the RawMessage to interface{}
		var payload interface{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			log.Printf("Failed to unmarshal payload for event %s: %v", event.ID, err)
			continue
		}

		// Construct a message to publish
		msg := struct {
			Type    string      `json:"type"`
			Payload interface{} `json:"payload"`
		}{
			Type:    event.EventType,
			Payload: payload,
		}

		// Fanout logic:
		// 1. Get Chat Members
		// 2. Publish to each member's routing key: user.{userID}

		// We need to extract ChatID from payload.
		// This assumes payload is domain.Message.
		// Ideally we should store metadata in outbox event or have a generic way.
		// For now, let's unmarshal to Message to get ChatID.
		var message domain.Message
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			log.Printf("Failed to unmarshal message for fanout: %v", err)
			continue
		}

		members, err := w.chatRepo.GetChatMembers(ctx, message.ChatID)
		if err != nil {
			log.Printf("Failed to get chat members: %v", err)
			continue
		}

		for _, memberID := range members {
			routingKey := fmt.Sprintf("user.%s", memberID)
			if err := w.broker.Publish(ctx, routingKey, msg); err != nil {
				log.Printf("Failed to publish event %s to %s: %v", event.ID, routingKey, err)
				// Should we fail the whole batch? Or just log?
				// For now log.
			}
		}

		processedIDs = append(processedIDs, event.ID)
	}

	if len(processedIDs) > 0 {
		if err := w.repo.MarkProcessed(ctx, tx, processedIDs); err != nil {
			log.Printf("Failed to mark events as processed: %v", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
	}
}
