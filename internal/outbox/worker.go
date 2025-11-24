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
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type StreamConsumer struct {
	broker       *broker.RabbitMQClient
	chatRepo     *repository.ChatRepository
	presenceRepo presence.Repository
	streamName   string
}

func NewStreamConsumer(broker *broker.RabbitMQClient, chatRepo *repository.ChatRepository, presenceRepo presence.Repository, streamName string) *StreamConsumer {
	return &StreamConsumer{
		broker:       broker,
		chatRepo:     chatRepo,
		presenceRepo: presenceRepo,
		streamName:   streamName,
	}
}

func (c *StreamConsumer) Start(ctx context.Context) error {
	// Consume from Stream using Native Client
	consumer, err := c.broker.StreamEnv.NewConsumer(
		c.streamName,
		func(consumerContext stream.ConsumerContext, message *amqp.Message) {
			c.processMessage(ctx, message)
		},
		stream.NewConsumerOptions().
			SetOffset(stream.OffsetSpecification{}.First()), // Start from beginning for now, or Last()
	)
	if err != nil {
		return fmt.Errorf("failed to start stream consumer: %w", err)
	}
	defer consumer.Close()

	log.Printf("Stream Consumer started on %s", c.streamName)

	<-ctx.Done()
	return nil
}

func (c *StreamConsumer) processMessage(ctx context.Context, msg *amqp.Message) {
	// Payload structure from RabbitMQStreamOutboxRepository
	var event struct {
		ID        uuid.UUID       `json:"id"`
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}

	if err := json.Unmarshal(msg.GetData(), &event); err != nil {
		log.Printf("Failed to unmarshal stream event: %v", err)
		return
	}

	// Fanout Logic (Same as before)

	// 1. Unmarshal Payload to get ChatID
	var message domain.Message
	if err := json.Unmarshal(event.Payload, &message); err != nil {
		// It might be a USER_JOINED event or something else
		if event.EventType == domain.EventTypeUserJoined {
			// Handle User Joined Fanout?
			// For now let's just log and skip if not a message
			// Or we can implement generic fanout if payload has chat_id
		}
		// If we can't parse as message, maybe we can parse as map to get chat_id
		var generic map[string]interface{}
		if err := json.Unmarshal(event.Payload, &generic); err == nil {
			if chatIDStr, ok := generic["chat_id"].(string); ok {
				message.ChatID, _ = uuid.Parse(chatIDStr)
			}
		}
	}

	if message.ChatID == uuid.Nil {
		// Skip if no chat ID
	}

	// 2. Get Members
	members, err := c.chatRepo.GetChatMembers(ctx, message.ChatID)
	if err != nil {
		log.Printf("Failed to get chat members: %v", err)
		return
	}

	// Prepare WS Message
	// We want to send the original event payload wrapped in {type, payload}
	// The event.Payload is already the inner object (e.g. Message)
	// We want to send: { type: "MESSAGE_CREATED", payload: {...} }

	// We need to unmarshal event.Payload to interface{} to wrap it
	var payloadObj interface{}
	json.Unmarshal(event.Payload, &payloadObj)

	wsMsg := struct {
		Type    string      `json:"type"`
		Payload interface{} `json:"payload"`
	}{
		Type:    event.EventType,
		Payload: payloadObj,
	}

	for _, memberID := range members {
		// Check Presence
		isOnline, err := c.presenceRepo.IsUserOnline(ctx, memberID)
		if err != nil {
			log.Printf("Failed to check presence: %v", err)
			isOnline = true // Fallback to try WS
		}

		if isOnline {
			routingKey := fmt.Sprintf("user.%s", memberID)
			if err := c.broker.Publish(ctx, routingKey, wsMsg); err != nil {
				log.Printf("Failed to publish to WS queue %s: %v", routingKey, err)
			}
		} else {
			// Offline -> Push
			routingKey := fmt.Sprintf("user.%s", memberID)
			if err := c.broker.PublishToExchange(ctx, "chat.push", routingKey, wsMsg); err != nil {
				log.Printf("Failed to publish push for %s: %v", memberID, err)
			}
		}
	}
}
