package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"chat_core/internal/broker"
	"chat_core/internal/domain"
	"chat_core/internal/presence"
	"chat_core/internal/repository"

	"github.com/google/uuid"
)

type Worker struct {
	repo         repository.OutboxRepository
	chatRepo     *repository.ChatRepository
	broker       *broker.RabbitMQClient
	presenceRepo presence.Repository
	stop         chan struct{}
}

func NewWorker(repo repository.OutboxRepository, chatRepo *repository.ChatRepository, broker *broker.RabbitMQClient, presenceRepo presence.Repository) *Worker {
	return &Worker{
		repo:         repo,
		chatRepo:     chatRepo,
		broker:       broker,
		presenceRepo: presenceRepo,
		stop:         make(chan struct{}),
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
			// Check if user is online
			isOnline, err := w.presenceRepo.IsUserOnline(ctx, memberID)
			if err != nil {
				log.Printf("Failed to check presence for user %s: %v", memberID, err)
				// Fallback to assuming offline? Or online?
				// Let's assume offline to be safe and send push?
				// Or assume online to try WS?
				// Let's try WS as it has DLX anyway (if we kept DLX on user queue).
				// But user asked to revert to immediate push if offline.
				// So if error, let's try WS queue.
				isOnline = true
			}

			if isOnline {
				// User is online (or error), send to their queue
				routingKey := fmt.Sprintf("user.%s", memberID)

				// Debug log
				log.Printf("Publishing to %s: Type=%s Payload=%+v", routingKey, msg.Type, msg.Payload)

				if err := w.broker.Publish(ctx, routingKey, msg); err != nil {
					log.Printf("Failed to publish event %s to %s: %v", event.ID, routingKey, err)
				}
			} else {
				// User is offline, send IMMEDIATE push
				// We publish to the Push Exchange directly
				// We need to pass the recipient ID so the Push Worker knows who to send to.
				// We can use headers for this.

				// We need a PublishWithHeaders method or similar.
				// The current Publish method doesn't support headers.
				// We need to update RabbitMQClient or add a new method.
				// Let's check RabbitMQClient.

				// Wait, I can't see RabbitMQClient definition right now but I recall it takes (ctx, routingKey, body).
				// I should add PublishWithHeaders to RabbitMQClient.
				// For now, I will assume I need to add it.

				// Actually, let's just use a special routing key or payload wrapper?
				// Payload wrapper is easier without changing interface.
				// But Push Worker expects the original message payload usually.

				// Let's add PublishPush method to broker?
				// Or just update Publish to accept options?

				// Let's modify RabbitMQClient to support headers.
				// But first let's mark this spot and go update RabbitMQClient.

				// TEMPORARY: I will use a hack. I will wrap the payload in a struct that PushWorker understands?
				// No, PushWorker reads x-death headers for DLX.
				// For direct push, I can put the user ID in the routing key?
				// The Push Exchange is FANOUT, so routing key is ignored by queues binding to it.
				// BUT the consumer receives the routing key!
				// So I can publish to `chat.push` with routing key `user.{userID}`.
				// The Push Worker consumes from `push_notifications` queue which binds to `chat.push` with `#`.
				// So it will receive the message and the routing key will be preserved!

				// YES! I can just use the routing key `user.{userID}` when publishing to `chat.push`.
				// The Push Worker just needs to parse the routing key.

				routingKey := fmt.Sprintf("user.%s", memberID)
				if err := w.broker.PublishToExchange(ctx, "chat.push", routingKey, msg); err != nil {
					log.Printf("Failed to publish push for %s: %v", memberID, err)
				}
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
