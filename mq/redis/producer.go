package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

// Publish 向指定的 Topic 发布消息
func (m *MessageQueue) Publish(body []byte) (string, error) {
	timeout := time.Second * time.Duration(m.config.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res := m.client.XAdd(ctx, &redis.XAddArgs{
		Stream: m.config.Topic,
		MaxLen: m.config.MaxLen,
		Approx: true,
		ID:     "*",
		Values: map[string][]byte{m.config.TransferKey: body},
	})
	err := res.Err()
	if err != nil {
		return "", err
	}
	messageId, err := res.Result()
	if err != nil {
		return "", err
	}
	return messageId, nil
}
