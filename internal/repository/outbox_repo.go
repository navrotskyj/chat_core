package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"chat_core/internal/broker"
	"chat_core/internal/domain"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type OutboxRepository interface {
	BeginTx(ctx context.Context) (*sql.Tx, error)
	Save(ctx context.Context, tx *sql.Tx, event *domain.OutboxEvent) error
	FetchPending(ctx context.Context, tx *sql.Tx, limit int) ([]*domain.OutboxEvent, error)
	MarkProcessed(ctx context.Context, tx *sql.Tx, ids []uuid.UUID) error
}

// ... Postgres implementation ...

// RabbitMQStreamOutboxRepository publishes events directly to a RabbitMQ Stream.
type RabbitMQStreamOutboxRepository struct {
	producer *stream.Producer
}

func NewRabbitMQStreamOutboxRepository(client *broker.RabbitMQClient, streamName string) (*RabbitMQStreamOutboxRepository, error) {
	producer, err := client.StreamEnv.NewProducer(streamName, stream.NewProducerOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to create stream producer: %w", err)
	}
	return &RabbitMQStreamOutboxRepository{
		producer: producer,
	}, nil
}

func (r *RabbitMQStreamOutboxRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	// We don't support SQL transactions for RabbitMQ, but we need to satisfy the interface.
	// In a real hybrid setup, we might return a DB tx here if we were also writing to DB.
	// For now, return nil which indicates "no transaction" or handle gracefully in Save.
	return nil, nil
}

func (r *RabbitMQStreamOutboxRepository) Save(ctx context.Context, tx *sql.Tx, event *domain.OutboxEvent) error {
	// We publish directly to the stream.
	// Note: This is not transactional with the DB tx!
	// Dual-write problem exists here.

	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create AMQP 1.0 message for Stream
	msg := amqp.NewMessage(payloadBytes)

	// We can set properties if needed
	// msg.SetApplicationProperties(...)

	if err := r.producer.Send(msg); err != nil {
		return fmt.Errorf("failed to publish to stream: %w", err)
	}

	return nil
}

func (r *RabbitMQStreamOutboxRepository) FetchPending(ctx context.Context, tx *sql.Tx, limit int) ([]*domain.OutboxEvent, error) {
	// Not used in Stream mode (Push model, not Pull)
	return nil, nil
}

func (r *RabbitMQStreamOutboxRepository) MarkProcessed(ctx context.Context, tx *sql.Tx, ids []uuid.UUID) error {
	// Not used in Stream mode
	return nil
}
