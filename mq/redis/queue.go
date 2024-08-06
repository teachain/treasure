package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/teachain/treasure/mq"
	"sync/atomic"
)

type Config struct {
	MaxLen      int64
	Count       int64
	Timeout     int
	BatchSize   int64
	Topic       string //主题
	GroupName   string //组名
	TransferKey string
}

type MessageQueue struct {
	client *redis.Client
	config *Config
	isStop int32
	stop   chan struct{}
}

func NewMessageQueue(client *redis.Client, config *Config) mq.MessageQueue {
	return &MessageQueue{
		client: client,
		config: config,
		isStop: 0,
		stop:   make(chan struct{}),
	}
}
func (m *MessageQueue) Shutdown() error {
	if atomic.CompareAndSwapInt32(&m.isStop, 0, 1) {
		close(m.stop)
		result := m.client.Shutdown(context.Background())
		return result.Err()
	}
	return nil
}
