package rocketmq

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/google/uuid"
	"github.com/teachain/treasure/mq"
)

func (r *RocketMessageQueue) StartConsumer(handler mq.MsgHandler) error {
	if r.consumer == nil {
		c, err := newConsumer(r.config)
		if err != nil {
			return err
		}
		r.consumer = c
	}
	err := r.consumer.Subscribe(r.config.Topic, consumer.MessageSelector{Type: consumer.TAG, Expression: r.config.Tag}, func(c context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			err := handler(msg.MsgId, msg.Body)
			if err != nil {
				return consumer.SuspendCurrentQueueAMoment, err
			}
		}
		return consumer.ConsumeSuccess, nil

	})
	if err != nil {
		return err
	}
	//开启消费者
	if err := r.consumer.Start(); err != nil {
		return err
	}
	return nil
}

func newConsumer(config *Config) (rocketmq.PushConsumer, error) {
	nameServer, err := primitive.NewNamesrvAddr(config.Addr...)
	consumerId := uuid.New().String()
	consumerObject, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(nameServer),
		consumer.WithInstance(fmt.Sprintf("%s-Consumer", consumerId)),
		consumer.WithGroupName(config.GroupName),
		consumer.WithPullInterval(time.Duration(config.PullInterval)*time.Millisecond), //拉消息间隔，单位需要是millisecond
		consumer.WithConsumerOrder(true),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerModel(consumer.Clustering), //消费模式，默认为clustering
		//一次消费多少条消息，默认值1，超过32就无意义了，这一批消息将拥有同一个消费状态，
		//即如果消息的消费状态返回的是CONSUME_SUCCESS，则它们都消费成功，
		//而如果返回的是RECONSUME_LATER，则它们都将再次投递。
		consumer.WithConsumeMessageBatchMaxSize(1),
		//最大重试次数，超过就放入死信DLQ队列，死信队列需要手动创建消费者去消费
		consumer.WithMaxReconsumeTimes(60),
	)
	if err != nil {
		return nil, err
	}
	return consumerObject, nil
}
func (r *RocketMessageQueue) Shutdown() error {
	if r.producer != nil {
		err := r.producer.Shutdown()
		if err != nil {
			return err
		}
	}
	if r.consumer != nil {
		err := r.consumer.Shutdown()
		if err != nil {
			return err
		}
	}
	return nil
}