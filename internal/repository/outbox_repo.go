package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"chat_core/internal/domain"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type OutboxRepository interface {
	BeginTx(ctx context.Context) (*sql.Tx, error)
	Save(ctx context.Context, tx *sql.Tx, event *domain.OutboxEvent) error
	FetchPending(ctx context.Context, tx *sql.Tx, limit int) ([]*domain.OutboxEvent, error)
	MarkProcessed(ctx context.Context, tx *sql.Tx, ids []uuid.UUID) error
}

type PostgresOutboxRepository struct {
	db *sql.DB
}

func NewPostgresOutboxRepository(db *sql.DB) *PostgresOutboxRepository {
	return &PostgresOutboxRepository{db: db}
}

func (r *PostgresOutboxRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, nil)
}

func (r *PostgresOutboxRepository) Save(ctx context.Context, tx *sql.Tx, event *domain.OutboxEvent) error {
	query := `
		INSERT INTO outbox (id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	// Use provided transaction if available, otherwise use db
	var exec interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}
	if tx != nil {
		exec = tx
	} else {
		exec = r.db
	}

	_, err := exec.ExecContext(ctx, query, event.ID, event.EventType, event.Payload, "pending", event.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	return nil
}

func (r *PostgresOutboxRepository) FetchPending(ctx context.Context, tx *sql.Tx, limit int) ([]*domain.OutboxEvent, error) {
	query := `
		SELECT id, event_type, payload, status, created_at
		FROM outbox
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, limit)
	} else {
		rows, err = r.db.QueryContext(ctx, query, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending events: %w", err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.Payload, &event.Status, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, &event)
	}
	return events, nil
}

func (r *PostgresOutboxRepository) MarkProcessed(ctx context.Context, tx *sql.Tx, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	query := `
		UPDATE outbox
		SET status = 'processed', processed_at = $1
		WHERE id = ANY($2)
	`

	var exec interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}
	if tx != nil {
		exec = tx
	} else {
		exec = r.db
	}

	_, err := exec.ExecContext(ctx, query, time.Now(), pq.Array(ids))
	if err != nil {
		return fmt.Errorf("failed to mark events as processed: %w", err)
	}
	return nil
}
