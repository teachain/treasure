package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/teachain/treasure/mq"
)

func (m *MessageQueue) StartConsumer(handler mq.MsgHandler) error {
	consumerId := uuid.New().String()
	ctx := context.Background()
	err := m.Consume(ctx, m.config.Topic, m.config.GroupName, consumerId, handler)
	if err != nil {
		return err
	}
	return nil
}

func (m *MessageQueue) Consume(ctx context.Context, topic, group, consumer string, handler mq.MsgHandler) error {
	// start 用于创建消费者组的时候指定起始消费ID，0表示从头开始消费，$表示从最后一条消息开始消费
	res := m.client.XGroupCreateMkStream(ctx, topic, group, "0")
	err := res.Err()

	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	go func() {
		for {
			select {
			case <-m.stop:
				return
			default:
			}
			// 拉取新消息
			if err := m.consume(ctx, topic, group, consumer, ">", m.config.BatchSize, handler); err != nil {
				return
			}
			// 拉取已经投递却未被ACK的消息，保证消息至少被成功消费1次
			if err := m.consume(ctx, topic, group, consumer, "0", m.config.BatchSize, handler); err != nil {
				return
			}
		}
	}()
	return nil
}

func (m *MessageQueue) consume(ctx context.Context, topic, group, consumer, id string, batchSize int64, handler mq.MsgHandler) error {
	result, err := m.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, id},
		Count:    batchSize,
	}).Result()
	if err != nil {
		return err
	}
	for _, msg := range result[0].Messages {
		if _, ok := msg.Values[m.config.TransferKey]; ok {
			value := msg.Values[m.config.TransferKey]
			data, ok := value.(string)
			if ok {
				err := handler(msg.ID, []byte(data))
				if err == nil {
					err := m.client.XAck(ctx, topic, group, msg.ID).Err()
					if err != nil {
						return err
					}
				}
			}
		} else {
			fmt.Println(fmt.Sprintf("message for %s not found", m.config.TransferKey))
		}
	}
	return nil
}
