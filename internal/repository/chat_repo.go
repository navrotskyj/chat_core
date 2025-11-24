package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"chat_core/internal/domain"

	"github.com/google/uuid"
)

type ChatRepository struct {
	db    *sql.DB
	cache sync.Map // map[uuid.UUID][]uuid.UUID
}

func NewChatRepository(db *sql.DB) *ChatRepository {
	return &ChatRepository{db: db}
}

func (r *ChatRepository) Invalidate(chatID uuid.UUID) {
	r.cache.Delete(chatID)
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
	// 1. Check Cache
	if val, ok := r.cache.Load(chatID); ok {
		return val.([]uuid.UUID), nil
	}

	// 2. Fetch from DB
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

	// 3. Update Cache
	r.cache.Store(chatID, members)

	return members, nil
}

func (r *ChatRepository) GetMessagesAfter(ctx context.Context, chatID uuid.UUID, afterID uuid.UUID, limit int) ([]*domain.Message, error) {
	var rows *sql.Rows
	var err error

	if afterID == uuid.Nil {
		// Fetch last N messages
		// We want them in ascending order, but we need the *last* N.
		// So we select desc limit N, then order back asc? Or just select all?
		// Usually sync without cursor means "give me initial state".
		// Let's just fetch the last N messages ordered by created_at DESC, then reverse them?
		// Or just fetch top N ordered by created_at DESC?
		// Let's do: SELECT * FROM messages WHERE chat_id = $1 ORDER BY created_at DESC LIMIT $2
		// And then reverse in code.
		query := `
			SELECT id, chat_id, sender_id, content, created_at
			FROM messages
			WHERE chat_id = $1
			ORDER BY created_at DESC
			LIMIT $2
		`
		rows, err = r.db.QueryContext(ctx, query, chatID, limit)
	} else {
		// Fetch messages after specific ID
		// We need the created_at of the afterID to compare efficiently,
		// or just use the ID if we assume monotonic IDs (UUIDv7) or just join.
		// Let's use a subquery for simplicity: created_at > (SELECT created_at FROM messages WHERE id = $2)
		query := `
			SELECT id, chat_id, sender_id, content, created_at
			FROM messages
			WHERE chat_id = $1
			  AND created_at > (SELECT created_at FROM messages WHERE id = $2)
			ORDER BY created_at ASC
			LIMIT $3
		`
		rows, err = r.db.QueryContext(ctx, query, chatID, afterID, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch messages: %w", err)
	}
	defer rows.Close()

	var messages []*domain.Message
	for rows.Next() {
		var msg domain.Message
		if err := rows.Scan(&msg.ID, &msg.ChatID, &msg.SenderID, &msg.Content, &msg.CreatedAt); err != nil {
			return nil, err
		}
		messages = append(messages, &msg)
	}

	// If we fetched initial history (desc), reverse it to be chronological
	if afterID == uuid.Nil {
		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
			messages[i], messages[j] = messages[j], messages[i]
		}
	}

	return messages, nil
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

func (r *ChatRepository) EnsureChatMember(ctx context.Context, chatID, userID uuid.UUID) (bool, error) {
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO chat_members (chat_id, user_id) VALUES ($1, $2)
		ON CONFLICT (chat_id, user_id) DO NOTHING
	`, chatID, userID)
	if err != nil {
		return false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}
