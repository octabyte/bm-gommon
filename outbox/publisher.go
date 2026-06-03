package outbox

import (
	"context"
	"fmt"
	"log"
	"time"
)

const maxBackoffDelay = 5 * time.Minute

// PublisherOptions configures outbox polling and retry behavior.
type PublisherOptions struct {
	BatchSize    int
	PollInterval time.Duration
	LockDuration time.Duration
	MaxAttempts  int
	WorkerID     string
}

// EventStore defines the persistence operations required by Publisher.
type EventStore interface {
	ClaimBatch(ctx context.Context, workerID string, batchSize int, lockDuration time.Duration) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, lastErr string) error
	RescheduleRetry(ctx context.Context, id string, nextRetry time.Time, lastErr string) error
}

// Publisher consumes pending outbox events and publishes them to a broker.
type Publisher struct {
	repo    EventStore
	brokers map[Destination]EventPublisher
	options PublisherOptions
	nowFn   func() time.Time
}

// NewPublisher creates an outbox publisher worker.
func NewPublisher(repo EventStore, brokers map[Destination]EventPublisher, options PublisherOptions) *Publisher {
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 2 * time.Second
	}
	if options.LockDuration <= 0 {
		options.LockDuration = 2 * time.Minute
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 10
	}

	return &Publisher{
		repo:    repo,
		brokers: brokers,
		options: options,
		nowFn:   time.Now,
	}
}

// Start runs the polling loop until the context is cancelled.
func (p *Publisher) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("outbox publisher stopped: %v", ctx.Err())
			return
		default:
		}

		processed, err := p.processBatch(ctx)
		if err != nil {
			log.Printf("outbox batch processing error: %v", err)
		}

		// When a full batch is processed, continue immediately to reduce latency.
		if processed >= p.options.BatchSize {
			continue
		}

		timer := time.NewTimer(p.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			log.Printf("outbox publisher stopped: %v", ctx.Err())
			return
		case <-timer.C:
		}
	}
}

func (p *Publisher) processBatch(ctx context.Context) (int, error) {
	events, err := p.repo.ClaimBatch(
		ctx,
		p.options.WorkerID,
		p.options.BatchSize,
		p.options.LockDuration,
	)
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}

	for _, event := range events {
		broker, ok := p.brokers[event.Destination]
		if !ok {
			destinationErr := fmt.Sprintf("no broker configured for destination: %s", event.Destination)
			if markErr := p.repo.MarkFailed(ctx, event.ID, destinationErr); markErr != nil {
				log.Printf("mark failed failed for outbox event %s: %v", event.ID, markErr)
			}
			log.Printf("outbox event moved to failed status: event_id=%s error=%s", event.ID, destinationErr)
			continue
		}

		err = broker.PublishRaw(ctx, event.EventName, event.Payload, event.Metadata)
		if err == nil {
			if markErr := p.repo.MarkPublished(ctx, event.ID); markErr != nil {
				log.Printf("mark published failed for outbox event %s: %v", event.ID, markErr)
			}
			continue
		}

		attemptNumber := event.Attempts + 1
		if attemptNumber >= p.options.MaxAttempts {
			if markErr := p.repo.MarkFailed(ctx, event.ID, err.Error()); markErr != nil {
				log.Printf("mark failed failed for outbox event %s: %v", event.ID, markErr)
			}
			log.Printf("outbox event moved to failed status: event_id=%s error=%v", event.ID, err)
			continue
		}

		nextRetryAt := p.nowFn().UTC().Add(backoffDuration(attemptNumber))
		if retryErr := p.repo.RescheduleRetry(ctx, event.ID, nextRetryAt, err.Error()); retryErr != nil {
			log.Printf("reschedule retry failed for outbox event %s: %v", event.ID, retryErr)
			continue
		}

		log.Printf(
			"outbox publish failed, scheduled retry: event_id=%s attempts=%d next_retry_at=%s error=%v",
			event.ID,
			attemptNumber,
			nextRetryAt.Format(time.RFC3339),
			err,
		)
	}

	return len(events), nil
}

func backoffDuration(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}

	seconds := 1 << (attempt - 1)
	delay := time.Duration(seconds) * time.Second
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}

	return delay
}
