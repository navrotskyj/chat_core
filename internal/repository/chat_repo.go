package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"chat_core/internal/domain"

	"github.com/google/uuid"
)

type ChatRepository struct {
	db *sql.DB
}

func NewChatRepository(db *sql.DB) *ChatRepository {
	return &ChatRepository{db: db}
}

func (r *ChatRepository) CreateMessage(ctx context.Context, msg *domain.Message) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Insert Message
	_, err = tx.ExecContext(ctx, `
		INSERT INTO messages (id, chat_id, sender_id, content, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`, msg.ID, msg.ChatID, msg.SenderID, msg.Content, msg.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert message: %w", err)
	}

	// 2. Create Outbox Event
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message payload: %w", err)
	}

	outboxRepo := &PostgresOutboxRepository{db: r.db} // We can reuse the struct logic but pass tx
	event := &domain.OutboxEvent{
		ID:        uuid.New(),
		EventType: domain.EventTypeMessageCreated,
		Payload:   payload,
		CreatedAt: time.Now(),
	}

	if err := outboxRepo.Save(ctx, tx, event); err != nil {
		return fmt.Errorf("failed to save outbox event: %w", err)
	}

	return tx.Commit()
}

func (r *ChatRepository) GetChatMembers(ctx context.Context, chatID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT user_id FROM chat_members WHERE chat_id = $1`, chatID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chat members: %w", err)
	}
	defer rows.Close()

	var members []uuid.UUID
	for rows.Next() {
		var userID uuid.UUID
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		members = append(members, userID)
	}
	return members, nil
}

func (r *ChatRepository) EnsureUser(ctx context.Context, userID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO users (id, username) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, userID, "user_"+userID.String()[:8])
	return err
}

func (r *ChatRepository) EnsureDevice(ctx context.Context, deviceID, userID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO devices (id, user_id, name) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
	`, deviceID, userID, "device_"+deviceID.String()[:8])
	return err
}

func (r *ChatRepository) EnsureChat(ctx context.Context, chatID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO chats (id, name) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, chatID, "chat_"+chatID.String()[:8])
	return err
}

func (r *ChatRepository) EnsureChatMember(ctx context.Context, chatID, userID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO chat_members (chat_id, user_id) VALUES ($1, $2)
		ON CONFLICT (chat_id, user_id) DO NOTHING
	`, chatID, userID)
	return err
}
