package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/octabyte/bm-gommon/outbox"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type outboxModel struct {
	ID          string             `gorm:"type:uuid;primaryKey"`
	EventName   string             `gorm:"type:varchar(255);not null"`
	Destination outbox.Destination `gorm:"type:varchar(32);not null"`
	Payload     []byte             `gorm:"type:jsonb;not null"`
	Metadata    []byte             `gorm:"type:jsonb"`
	Status      string             `gorm:"type:varchar(32);not null;index:idx_outbox_status_next_retry,priority:1"`
	Attempts    int                `gorm:"not null;default:0"`
	NextRetryAt time.Time          `gorm:"type:timestamptz;not null;index:idx_outbox_status_next_retry,priority:2"`
	LockedBy    *string            `gorm:"type:text"`
	LockedUntil *time.Time         `gorm:"type:timestamptz"`
	LastError   *string            `gorm:"type:text"`
	CreatedAt   time.Time          `gorm:"type:timestamptz;not null"`
	UpdatedAt   time.Time          `gorm:"type:timestamptz;not null"`
	PublishedAt *time.Time         `gorm:"type:timestamptz"`
}

func (outboxModel) TableName() string {
	return outbox.TableName
}

// Repository persists and claims outbox events from PostgreSQL.
type Repository struct {
	db    *gorm.DB
	nowFn func() time.Time
}

// NewRepository creates a new PostgreSQL outbox repository.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{
		db:    db,
		nowFn: time.Now,
	}
}

// WithTx returns a repository bound to the provided transaction.
func (r *Repository) WithTx(tx *gorm.DB) *Repository {
	return &Repository{
		db:    tx,
		nowFn: r.nowFn,
	}
}

// Migrate creates/updates the outbox schema.
func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&outboxModel{})
}

// Enqueue persists an event in pending status so it can be published later.
func (r *Repository) Enqueue(ctx context.Context, eventName string, payload any, metadata map[string]any, opts ...outbox.EnqueueOption) error {
	cfg := outbox.EnqueueConfig{
		Destination: outbox.DestStream,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	rawMetadata, err := marshalMetadata(metadata)
	if err != nil {
		return fmt.Errorf("marshal outbox metadata: %w", err)
	}

	now := r.nowFn().UTC()
	event := outboxModel{
		ID:          uuid.NewString(),
		EventName:   eventName,
		Destination: cfg.Destination,
		Payload:     rawPayload,
		Metadata:    rawMetadata,
		Status:      outbox.StatusPending,
		Attempts:    0,
		NextRetryAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	err = r.db.WithContext(ctx).Create(&event).Error
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	return nil
}

// ClaimBatch claims a batch of pending (or expired-processing) events for a worker.
func (r *Repository) ClaimBatch(
	ctx context.Context,
	workerID string,
	batchSize int,
	lockDuration time.Duration,
) ([]outbox.OutboxEvent, error) {
	if batchSize <= 0 {
		return nil, nil
	}

	now := r.nowFn().UTC()
	lockUntil := now.Add(lockDuration)
	claimed := make([]outboxModel, 0, batchSize)

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		candidates := make([]outboxModel, 0, batchSize)
		err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(
				"(status = ? AND next_retry_at <= ?) OR (status = ? AND locked_until <= ? AND next_retry_at <= ?)",
				outbox.StatusPending, now,
				outbox.StatusProcessing, now, now,
			).
			Order("created_at ASC").
			Limit(batchSize).
			Find(&candidates).Error
		if err != nil {
			return fmt.Errorf("find claim candidates: %w", err)
		}

		if len(candidates) == 0 {
			return nil
		}

		ids := make([]string, 0, len(candidates))
		for _, event := range candidates {
			ids = append(ids, event.ID)
		}

		update := map[string]any{
			"status":       outbox.StatusProcessing,
			"locked_by":    workerID,
			"locked_until": lockUntil,
			"updated_at":   now,
		}
		if err := tx.Model(&outboxModel{}).Where("id IN ?", ids).Updates(update).Error; err != nil {
			return fmt.Errorf("claim outbox events: %w", err)
		}

		if err := tx.Where("id IN ?", ids).Order("created_at ASC").Find(&claimed).Error; err != nil {
			return fmt.Errorf("load claimed outbox events: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	events := make([]outbox.OutboxEvent, 0, len(claimed))
	for _, event := range claimed {
		domainEvent, convErr := toDomain(event)
		if convErr != nil {
			return nil, convErr
		}
		events = append(events, domainEvent)
	}

	return events, nil
}

// MarkPublished marks an event as successfully published.
func (r *Repository) MarkPublished(ctx context.Context, id string) error {
	now := r.nowFn().UTC()
	update := map[string]any{
		"status":       outbox.StatusPublished,
		"published_at": now,
		"updated_at":   now,
		"locked_by":    nil,
		"locked_until": nil,
		"last_error":   nil,
	}

	err := r.db.WithContext(ctx).Model(&outboxModel{}).Where("id = ?", id).Updates(update).Error
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}

	return nil
}

// MarkFailed marks an event as failed after exhausting retries.
func (r *Repository) MarkFailed(ctx context.Context, id string, lastErr string) error {
	now := r.nowFn().UTC()
	update := map[string]any{
		"status":       outbox.StatusFailed,
		"last_error":   lastErr,
		"updated_at":   now,
		"locked_by":    nil,
		"locked_until": nil,
	}

	err := r.db.WithContext(ctx).Model(&outboxModel{}).Where("id = ?", id).Updates(update).Error
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}

	return nil
}

// RescheduleRetry updates retry metadata after a publish failure.
func (r *Repository) RescheduleRetry(
	ctx context.Context,
	id string,
	nextRetry time.Time,
	lastErr string,
) error {
	now := r.nowFn().UTC()
	update := map[string]any{
		"status":        outbox.StatusPending,
		"next_retry_at": nextRetry.UTC(),
		"last_error":    lastErr,
		"updated_at":    now,
		"locked_by":     nil,
		"locked_until":  nil,
		"attempts":      gorm.Expr("attempts + 1"),
	}

	err := r.db.WithContext(ctx).Model(&outboxModel{}).Where("id = ?", id).Updates(update).Error
	if err != nil {
		return fmt.Errorf("reschedule outbox retry: %w", err)
	}

	return nil
}

func toDomain(event outboxModel) (outbox.OutboxEvent, error) {
	metadata, err := unmarshalMetadata(event.Metadata)
	if err != nil {
		return outbox.OutboxEvent{}, fmt.Errorf("unmarshal outbox metadata: %w", err)
	}

	return outbox.OutboxEvent{
		ID:          event.ID,
		EventName:   event.EventName,
		Destination: event.Destination,
		Payload:     event.Payload,
		Metadata:    metadata,
		Status:      event.Status,
		Attempts:    event.Attempts,
		NextRetryAt: event.NextRetryAt,
		LockedBy:    event.LockedBy,
		LockedUntil: event.LockedUntil,
		LastError:   event.LastError,
		CreatedAt:   event.CreatedAt,
		UpdatedAt:   event.UpdatedAt,
		PublishedAt: event.PublishedAt,
	}, nil
}

func marshalMetadata(metadata map[string]any) ([]byte, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	return json.Marshal(metadata)
}

func unmarshalMetadata(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, err
	}

	return metadata, nil
}
