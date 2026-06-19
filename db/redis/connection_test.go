package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type RedisConnectionUnitSuite struct {
	suite.Suite
}

func TestRedisConnectionUnitSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(RedisConnectionUnitSuite))
}

func (s *RedisConnectionUnitSuite) TestPingWithRetryFunc_SucceedsAfterRetries() {
	attempts := 0
	cfg := Config{
		ConnectTimeout:   50 * time.Millisecond,
		RetryInitial:     5 * time.Millisecond,
		RetryMaxInterval: 20 * time.Millisecond,
		RetryMaxElapsed:  200 * time.Millisecond,
	}

	err := pingWithRetryFunc(context.Background(), cfg, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 3, attempts)
}

func (s *RedisConnectionUnitSuite) TestPingWithRetryFunc_FailsAfterMaxElapsed() {
	cfg := Config{
		ConnectTimeout:   20 * time.Millisecond,
		RetryInitial:     5 * time.Millisecond,
		RetryMaxInterval: 10 * time.Millisecond,
		RetryMaxElapsed:  30 * time.Millisecond,
	}

	err := pingWithRetryFunc(context.Background(), cfg, func(context.Context) error {
		return errors.New("still failing")
	})
	require.Error(s.T(), err)
}

func (s *RedisConnectionUnitSuite) TestPingWithRetryFunc_DisableRetryWithNegativeMaxElapsed() {
	attempts := 0
	cfg := Config{
		ConnectTimeout:  20 * time.Millisecond,
		RetryMaxElapsed: -1,
	}

	err := pingWithRetryFunc(context.Background(), cfg, func(context.Context) error {
		attempts++
		return errors.New("single attempt failure")
	})
	require.Error(s.T(), err)
	require.Equal(s.T(), 1, attempts)
}

type RedisConnectionIntegrationSuite struct {
	suite.Suite

	ctx       context.Context
	container testcontainers.Container
	client    *goredis.Client
	addr      string
}

func TestRedisConnectionIntegrationSuite(t *testing.T) {
	suite.Run(t, new(RedisConnectionIntegrationSuite))
}

func (s *RedisConnectionIntegrationSuite) SetupSuite() {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.T().Skipf("skipping redis integration tests, docker unavailable: %v", recovered)
		}
	}()

	s.ctx = context.Background()

	container, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.2-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		s.T().Skipf("skipping redis integration tests, redis container unavailable: %v", err)
	}

	host, err := container.Host(s.ctx)
	require.NoError(s.T(), err)

	port, err := container.MappedPort(s.ctx, "6379/tcp")
	require.NoError(s.T(), err)

	s.addr = fmt.Sprintf("%s:%s", host, port.Port())
	s.container = container
}

func (s *RedisConnectionIntegrationSuite) TearDownSuite() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.container != nil {
		_ = s.container.Terminate(s.ctx)
	}
}

func (s *RedisConnectionIntegrationSuite) SetupTest() {
	if s.client != nil {
		_ = s.client.Close()
	}

	s.client = NewRedisClient(s.ctx, Config{
		Addr:             s.addr,
		ConnectTimeout:   2 * time.Second,
		RetryInitial:     100 * time.Millisecond,
		RetryMaxInterval: 500 * time.Millisecond,
		RetryMaxElapsed:  10 * time.Second,
	})

	require.NotNil(s.T(), s.client)
	require.NoError(s.T(), s.client.FlushDB(s.ctx).Err())
}

func (s *RedisConnectionIntegrationSuite) TestNewRedisClient_ConnectsAndPerformsOperations() {
	err := Ping(s.ctx, s.client)
	require.NoError(s.T(), err)

	err = Set(s.ctx, s.client, "integration:key", "value", time.Minute)
	require.NoError(s.T(), err)

	got, err := Get(s.ctx, s.client, "integration:key")
	require.NoError(s.T(), err)
	require.Equal(s.T(), "value", got)
}

func (s *RedisConnectionIntegrationSuite) TestClient_ReconnectsAfterContainerRestart() {
	err := Set(s.ctx, s.client, "reconnect:key", "before-restart", time.Minute)
	require.NoError(s.T(), err)

	err = s.container.Stop(s.ctx, nil)
	require.NoError(s.T(), err)

	downCtx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
	defer cancel()
	require.Error(s.T(), s.client.Ping(downCtx).Err())

	err = s.container.Start(s.ctx)
	require.NoError(s.T(), err)

	require.Eventually(s.T(), func() bool {
		pingCtx, cancel := context.WithTimeout(s.ctx, time.Second)
		defer cancel()
		return s.client.Ping(pingCtx).Err() == nil
	}, 15*time.Second, 250*time.Millisecond, "client should reconnect after redis restart")

	err = Set(s.ctx, s.client, "reconnect:key", "after-restart", time.Minute)
	require.NoError(s.T(), err)

	got, err := Get(s.ctx, s.client, "reconnect:key")
	require.NoError(s.T(), err)
	require.Equal(s.T(), "after-restart", got)
}
