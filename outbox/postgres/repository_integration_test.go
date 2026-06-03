package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/octabyte/bm-gommon/outbox"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const testDBName = "outbox_test"

type RepositoryIntegrationSuite struct {
	suite.Suite

	ctx       context.Context
	db        *gorm.DB
	container testcontainers.Container
	repo      *Repository
}

func TestRepositoryIntegrationSuite(t *testing.T) {
	suite.Run(t, new(RepositoryIntegrationSuite))
}

func (s *RepositoryIntegrationSuite) SetupSuite() {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.T().Skipf("skipping outbox integration tests, docker unavailable: %v", recovered)
		}
	}()

	s.ctx = context.Background()

	container, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_DB":       testDBName,
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		s.T().Skipf("skipping outbox integration tests, postgres container unavailable: %v", err)
	}

	host, err := container.Host(s.ctx)
	require.NoError(s.T(), err)

	port, err := container.MappedPort(s.ctx, "5432/tcp")
	require.NoError(s.T(), err)

	dsn := fmt.Sprintf(
		"host=%s port=%s user=postgres password=postgres dbname=%s sslmode=disable TimeZone=UTC",
		host,
		port.Port(),
		testDBName,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(s.T(), err)
	require.NoError(s.T(), Migrate(db))

	s.container = container
	s.db = db
	s.repo = NewRepository(db)
}

func (s *RepositoryIntegrationSuite) TearDownSuite() {
	if s.container != nil {
		_ = s.container.Terminate(s.ctx)
	}
}

func (s *RepositoryIntegrationSuite) SetupTest() {
	s.db.Exec("TRUNCATE TABLE " + outbox.TableName)
}

func (s *RepositoryIntegrationSuite) TestEnqueue_PersistsPendingEvent() {
	err := s.repo.Enqueue(s.ctx, "schedule.booking.created", map[string]any{
		"booking_id": "booking-123",
	}, map[string]any{
		"company_id": 77,
	})
	require.NoError(s.T(), err)

	var events []outboxModel
	err = s.db.WithContext(s.ctx).Find(&events).Error
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 1)

	event := events[0]
	require.Equal(s.T(), outbox.StatusPending, event.Status)
	require.Equal(s.T(), 0, event.Attempts)
	require.Equal(s.T(), "schedule.booking.created", event.EventName)
	require.Equal(s.T(), outbox.DestStream, event.Destination)
	require.NotEmpty(s.T(), event.ID)
	require.JSONEq(s.T(), `{"booking_id":"booking-123"}`, string(event.Payload))

	metadata := map[string]any{}
	require.NoError(s.T(), json.Unmarshal(event.Metadata, &metadata))
	require.Equal(s.T(), float64(77), metadata["company_id"])
	require.NotZero(s.T(), event.CreatedAt)
	require.NotZero(s.T(), event.NextRetryAt)
}

func (s *RepositoryIntegrationSuite) TestEnqueue_WithDestinationOption_PersistsDestination() {
	err := s.repo.Enqueue(
		s.ctx,
		"schedule.booking.created",
		map[string]any{"booking_id": "booking-123"},
		nil,
		outbox.WithDestination(outbox.DestQueue),
	)
	require.NoError(s.T(), err)

	var event outboxModel
	err = s.db.WithContext(s.ctx).First(&event).Error
	require.NoError(s.T(), err)
	require.Equal(s.T(), outbox.DestQueue, event.Destination)
}

func (s *RepositoryIntegrationSuite) TestClaimBatch_ClaimsOnlyPendingAndExpiredProcessing() {
	now := time.Now().UTC()
	futureLock := now.Add(5 * time.Minute)
	expiredLock := now.Add(-1 * time.Minute)

	pendingA := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.pending.a",
		Payload:     []byte(`{"a":1}`),
		Status:      outbox.StatusPending,
		Attempts:    0,
		NextRetryAt: now.Add(-2 * time.Minute),
		CreatedAt:   now.Add(-4 * time.Minute),
		UpdatedAt:   now.Add(-4 * time.Minute),
	})
	pendingB := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.pending.b",
		Payload:     []byte(`{"b":1}`),
		Status:      outbox.StatusPending,
		Attempts:    0,
		NextRetryAt: now.Add(-1 * time.Minute),
		CreatedAt:   now.Add(-3 * time.Minute),
		UpdatedAt:   now.Add(-3 * time.Minute),
	})
	expired := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.expired",
		Payload:     []byte(`{"c":1}`),
		Status:      outbox.StatusProcessing,
		Attempts:    2,
		NextRetryAt: now.Add(-1 * time.Minute),
		LockedBy:    strPtr("old-worker"),
		LockedUntil: &expiredLock,
		CreatedAt:   now.Add(-2 * time.Minute),
		UpdatedAt:   now.Add(-2 * time.Minute),
	})
	_ = s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.active.locked",
		Payload:     []byte(`{"d":1}`),
		Status:      outbox.StatusProcessing,
		Attempts:    1,
		NextRetryAt: now.Add(-1 * time.Minute),
		LockedBy:    strPtr("other-worker"),
		LockedUntil: &futureLock,
		CreatedAt:   now.Add(-1 * time.Minute),
		UpdatedAt:   now.Add(-1 * time.Minute),
	})

	claimed, err := s.repo.ClaimBatch(s.ctx, "worker-A", 3, 2*time.Minute)
	require.NoError(s.T(), err)
	require.Len(s.T(), claimed, 3)

	require.Equal(s.T(), pendingA.ID, claimed[0].ID)
	require.Equal(s.T(), pendingB.ID, claimed[1].ID)
	require.Equal(s.T(), expired.ID, claimed[2].ID)

	for _, event := range claimed {
		require.Equal(s.T(), outbox.StatusProcessing, event.Status)
		require.NotNil(s.T(), event.LockedBy)
		require.Equal(s.T(), "worker-A", *event.LockedBy)
		require.NotNil(s.T(), event.LockedUntil)
		require.True(s.T(), event.LockedUntil.After(now))
	}
}

func (s *RepositoryIntegrationSuite) TestClaimBatch_DifferentWorkers_DoNotDuplicateClaims() {
	now := time.Now().UTC()
	const totalEvents = 50
	for i := 0; i < totalEvents; i++ {
		s.insertEvent(outboxModel{
			ID:          uuid.NewString(),
			EventName:   "event.pending",
			Payload:     []byte(fmt.Sprintf(`{"index":%d}`, i)),
			Status:      outbox.StatusPending,
			NextRetryAt: now.Add(-time.Minute),
			CreatedAt:   now.Add(time.Duration(i) * time.Millisecond),
			UpdatedAt:   now.Add(time.Duration(i) * time.Millisecond),
		})
	}

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		claimedMap = make(map[string]int)
	)

	claimFn := func(workerID string) {
		defer wg.Done()
		events, err := s.repo.ClaimBatch(s.ctx, workerID, totalEvents, 2*time.Minute)
		require.NoError(s.T(), err)

		mu.Lock()
		defer mu.Unlock()
		for _, event := range events {
			claimedMap[event.ID]++
		}
	}

	wg.Add(2)
	go claimFn("worker-A")
	go claimFn("worker-B")
	wg.Wait()

	require.Len(s.T(), claimedMap, totalEvents)
	for _, claimCount := range claimedMap {
		require.Equal(s.T(), 1, claimCount, "an event was claimed by more than one worker")
	}
}

func (s *RepositoryIntegrationSuite) TestMarkPublished_TransitionsAndClearsLocks() {
	now := time.Now().UTC()
	lockUntil := now.Add(2 * time.Minute)
	event := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.name",
		Payload:     []byte(`{"x":1}`),
		Status:      outbox.StatusProcessing,
		Attempts:    1,
		NextRetryAt: now.Add(-time.Minute),
		LockedBy:    strPtr("worker-A"),
		LockedUntil: &lockUntil,
		CreatedAt:   now.Add(-2 * time.Minute),
		UpdatedAt:   now.Add(-2 * time.Minute),
	})

	err := s.repo.MarkPublished(s.ctx, event.ID)
	require.NoError(s.T(), err)

	dbEvent := s.getEvent(event.ID)
	require.Equal(s.T(), outbox.StatusPublished, dbEvent.Status)
	require.NotNil(s.T(), dbEvent.PublishedAt)
	require.Nil(s.T(), dbEvent.LockedBy)
	require.Nil(s.T(), dbEvent.LockedUntil)
	require.Nil(s.T(), dbEvent.LastError)
}

func (s *RepositoryIntegrationSuite) TestRescheduleRetry_IncrementsAttemptsAndRequeues() {
	now := time.Now().UTC()
	lockUntil := now.Add(2 * time.Minute)
	event := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.name",
		Payload:     []byte(`{"x":1}`),
		Status:      outbox.StatusProcessing,
		Attempts:    2,
		NextRetryAt: now.Add(-time.Minute),
		LockedBy:    strPtr("worker-A"),
		LockedUntil: &lockUntil,
		CreatedAt:   now.Add(-2 * time.Minute),
		UpdatedAt:   now.Add(-2 * time.Minute),
	})

	nextRetry := now.Add(30 * time.Second)
	lastErr := "stream unavailable"
	err := s.repo.RescheduleRetry(s.ctx, event.ID, nextRetry, lastErr)
	require.NoError(s.T(), err)

	dbEvent := s.getEvent(event.ID)
	require.Equal(s.T(), outbox.StatusPending, dbEvent.Status)
	require.Equal(s.T(), 3, dbEvent.Attempts)
	require.WithinDuration(s.T(), nextRetry, dbEvent.NextRetryAt, time.Second)
	require.Nil(s.T(), dbEvent.LockedBy)
	require.Nil(s.T(), dbEvent.LockedUntil)
	require.NotNil(s.T(), dbEvent.LastError)
	require.Equal(s.T(), lastErr, *dbEvent.LastError)
}

func (s *RepositoryIntegrationSuite) TestMarkFailed_SetsFailedAndClearsLocks() {
	now := time.Now().UTC()
	lockUntil := now.Add(2 * time.Minute)
	event := s.insertEvent(outboxModel{
		ID:          uuid.NewString(),
		EventName:   "event.name",
		Payload:     []byte(`{"x":1}`),
		Status:      outbox.StatusProcessing,
		Attempts:    10,
		NextRetryAt: now.Add(-time.Minute),
		LockedBy:    strPtr("worker-A"),
		LockedUntil: &lockUntil,
		CreatedAt:   now.Add(-2 * time.Minute),
		UpdatedAt:   now.Add(-2 * time.Minute),
	})

	err := s.repo.MarkFailed(s.ctx, event.ID, "max retries reached")
	require.NoError(s.T(), err)

	dbEvent := s.getEvent(event.ID)
	require.Equal(s.T(), outbox.StatusFailed, dbEvent.Status)
	require.Nil(s.T(), dbEvent.LockedBy)
	require.Nil(s.T(), dbEvent.LockedUntil)
	require.NotNil(s.T(), dbEvent.LastError)
	require.Equal(s.T(), "max retries reached", *dbEvent.LastError)
}

func (s *RepositoryIntegrationSuite) insertEvent(event outboxModel) outboxModel {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if event.UpdatedAt.IsZero() {
		event.UpdatedAt = event.CreatedAt
	}
	if event.Destination == "" {
		event.Destination = outbox.DestStream
	}

	err := s.db.WithContext(s.ctx).Create(&event).Error
	require.NoError(s.T(), err)
	return event
}

func (s *RepositoryIntegrationSuite) getEvent(id string) outboxModel {
	var event outboxModel
	err := s.db.WithContext(s.ctx).First(&event, "id = ?", id).Error
	require.NoError(s.T(), err)
	return event
}

func strPtr(value string) *string {
	return &value
}
