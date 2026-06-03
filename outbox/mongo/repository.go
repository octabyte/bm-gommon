package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/octabyte/bm-gommon/outbox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoEvent struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	EventName   string             `bson:"event_name"`
	Destination outbox.Destination `bson:"destination"`
	Payload     json.RawMessage    `bson:"payload"`
	Metadata    map[string]any     `bson:"metadata,omitempty"`
	Status      string             `bson:"status"`
	Attempts    int                `bson:"attempts"`
	NextRetryAt time.Time          `bson:"next_retry_at"`
	LockedBy    *string            `bson:"locked_by,omitempty"`
	LockedUntil *time.Time         `bson:"locked_until,omitempty"`
	LastError   *string            `bson:"last_error,omitempty"`
	CreatedAt   time.Time          `bson:"created_at"`
	UpdatedAt   time.Time          `bson:"updated_at"`
	PublishedAt *time.Time         `bson:"published_at,omitempty"`
}

// Repository persists and claims outbox events from MongoDB.
type Repository struct {
	dbClient *mongo.Client
	dbName   string
	nowFn    func() time.Time
}

// NewRepository creates a new MongoDB outbox repository.
func NewRepository(dbClient *mongo.Client, dbName string) *Repository {
	return &Repository{
		dbClient: dbClient,
		dbName:   dbName,
		nowFn:    time.Now,
	}
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

	now := r.nowFn().UTC()
	docID := primitive.NewObjectID()
	event := mongoEvent{
		ID:          docID,
		EventName:   eventName,
		Destination: cfg.Destination,
		Payload:     rawPayload,
		Metadata:    metadata,
		Status:      outbox.StatusPending,
		Attempts:    0,
		NextRetryAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	_, err = r.collection().InsertOne(ctx, event)
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
	eligibilityFilter := eligibleFilter(now)

	type candidate struct {
		ID primitive.ObjectID `bson:"_id"`
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: 1}}).
		SetLimit(int64(batchSize)).
		SetProjection(bson.M{"_id": 1})

	cursor, err := r.collection().Find(ctx, eligibilityFilter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("find claim candidates: %w", err)
	}
	defer cursor.Close(ctx)

	var candidates []candidate
	if err = cursor.All(ctx, &candidates); err != nil {
		return nil, fmt.Errorf("decode claim candidates: %w", err)
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	candidateIDs := make([]primitive.ObjectID, 0, len(candidates))
	for _, item := range candidates {
		candidateIDs = append(candidateIDs, item.ID)
	}

	claimFilter := bson.M{
		"_id": bson.M{"$in": candidateIDs},
		"$or": eligibilityFilter["$or"],
	}
	claimUpdate := bson.M{
		"$set": bson.M{
			"status":       outbox.StatusProcessing,
			"locked_by":    workerID,
			"locked_until": lockUntil,
			"updated_at":   now,
		},
	}

	updateResult, err := r.collection().UpdateMany(ctx, claimFilter, claimUpdate)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}

	if updateResult.ModifiedCount == 0 {
		return nil, nil
	}

	claimedFilter := bson.M{
		"_id":          bson.M{"$in": candidateIDs},
		"status":       outbox.StatusProcessing,
		"locked_by":    workerID,
		"locked_until": bson.M{"$gte": now},
	}
	claimedOpts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}})
	claimedCursor, err := r.collection().Find(ctx, claimedFilter, claimedOpts)
	if err != nil {
		return nil, fmt.Errorf("load claimed outbox events: %w", err)
	}
	defer claimedCursor.Close(ctx)

	events := make([]mongoEvent, 0, batchSize)
	if err = claimedCursor.All(ctx, &events); err != nil {
		return nil, fmt.Errorf("decode claimed outbox events: %w", err)
	}

	domainEvents := make([]outbox.OutboxEvent, 0, len(events))
	for _, event := range events {
		domainEvents = append(domainEvents, toDomain(event))
	}

	return domainEvents, nil
}

// MarkPublished marks an event as successfully published.
func (r *Repository) MarkPublished(ctx context.Context, id string) error {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid outbox id: %w", err)
	}

	now := r.nowFn().UTC()
	update := bson.M{
		"$set": bson.M{
			"status":       outbox.StatusPublished,
			"published_at": now,
			"updated_at":   now,
		},
		"$unset": bson.M{
			"locked_by":    "",
			"locked_until": "",
			"last_error":   "",
		},
	}

	_, err = r.collection().UpdateByID(ctx, objectID, update)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}

	return nil
}

// MarkFailed marks an event as failed after exhausting retries.
func (r *Repository) MarkFailed(ctx context.Context, id string, lastErr string) error {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid outbox id: %w", err)
	}

	now := r.nowFn().UTC()
	update := bson.M{
		"$set": bson.M{
			"status":     outbox.StatusFailed,
			"last_error": lastErr,
			"updated_at": now,
		},
		"$unset": bson.M{
			"locked_by":    "",
			"locked_until": "",
		},
	}

	_, err = r.collection().UpdateByID(ctx, objectID, update)
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
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid outbox id: %w", err)
	}

	now := r.nowFn().UTC()
	update := bson.M{
		"$set": bson.M{
			"status":        outbox.StatusPending,
			"next_retry_at": nextRetry.UTC(),
			"last_error":    lastErr,
			"updated_at":    now,
		},
		"$inc": bson.M{
			"attempts": 1,
		},
		"$unset": bson.M{
			"locked_by":    "",
			"locked_until": "",
		},
	}

	_, err = r.collection().UpdateByID(ctx, objectID, update)
	if err != nil {
		return fmt.Errorf("reschedule outbox retry: %w", err)
	}

	return nil
}

func (r *Repository) collection() *mongo.Collection {
	return r.dbClient.Database(r.dbName).Collection(outbox.CollectionName)
}

func eligibleFilter(now time.Time) bson.M {
	return bson.M{
		"$or": bson.A{
			bson.M{
				"status":        outbox.StatusPending,
				"next_retry_at": bson.M{"$lte": now},
			},
			bson.M{
				"status":        outbox.StatusProcessing,
				"locked_until":  bson.M{"$lte": now},
				"next_retry_at": bson.M{"$lte": now},
			},
		},
	}
}

func toDomain(event mongoEvent) outbox.OutboxEvent {
	return outbox.OutboxEvent{
		ID:          event.ID.Hex(),
		EventName:   event.EventName,
		Destination: event.Destination,
		Payload:     event.Payload,
		Metadata:    event.Metadata,
		Status:      event.Status,
		Attempts:    event.Attempts,
		NextRetryAt: event.NextRetryAt,
		LockedBy:    event.LockedBy,
		LockedUntil: event.LockedUntil,
		LastError:   event.LastError,
		CreatedAt:   event.CreatedAt,
		UpdatedAt:   event.UpdatedAt,
		PublishedAt: event.PublishedAt,
	}
}
