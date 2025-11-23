package presence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type Repository interface {
	AddSession(ctx context.Context, userID, deviceID uuid.UUID, nodeID string) error
	RemoveSession(ctx context.Context, userID, deviceID uuid.UUID) error
	IsUserOnline(ctx context.Context, userID uuid.UUID) (bool, error)
}

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) AddSession(ctx context.Context, userID, deviceID uuid.UUID, nodeID string) error {
	query := `
		INSERT INTO active_sessions (user_id, device_id, node_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id, device_id) DO UPDATE
		SET node_id = $3, connected_at = NOW()
	`
	_, err := r.db.ExecContext(ctx, query, userID, deviceID, nodeID)
	if err != nil {
		return fmt.Errorf("failed to add session: %w", err)
	}
	return nil
}

func (r *PostgresRepository) RemoveSession(ctx context.Context, userID, deviceID uuid.UUID) error {
	query := `
		DELETE FROM active_sessions
		WHERE user_id = $1 AND device_id = $2
	`
	_, err := r.db.ExecContext(ctx, query, userID, deviceID)
	if err != nil {
		return fmt.Errorf("failed to remove session: %w", err)
	}
	return nil
}

func (r *PostgresRepository) IsUserOnline(ctx context.Context, userID uuid.UUID) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM active_sessions WHERE user_id = $1)`
	var exists bool
	err := r.db.QueryRowContext(ctx, query, userID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if user is online: %w", err)
	}
	return exists, nil
}
