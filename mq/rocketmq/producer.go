package rocketmq

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func (r *RocketMessageQueue) Publish(body []byte) (string, error) {
	if r.producer == nil {
		publisher, err := newProducer(r.config)
		if err != nil {
			return "", err
		}
		r.producer = publisher
	}
	msg := primitive.NewMessage(r.config.Topic, body).WithTag(r.config.Tag)
	result, err := r.producer.SendSync(context.Background(), msg)
	if err != nil {
		return "", err
	}
	return result.MsgID, nil
}

func newProducer(config *Config) (rocketmq.Producer, error) {
	//TODO:对域名进行解析
	nameServer, err := primitive.NewNamesrvAddr(config.Addr...)
	publisher, err := rocketmq.NewProducer(
		producer.WithInstanceName(config.GroupName+"Producer"),
		producer.WithGroupName(config.GroupName),
		producer.WithNameServer(nameServer),
		producer.WithRetry(config.Retries),
	)
	if err != nil {
		return nil, err
	}
	//启动消费者
	if err = publisher.Start(); err != nil {
		return nil, err
	}
	//健康检查
	msg := primitive.NewMessage(config.Topic, []byte("startTime:"+time.Now().Format(time.DateTime)))
	msg.WithKeys([]string{config.Topic + ":pid:" + strconv.Itoa(os.Getpid())})
	_, err = publisher.SendSync(context.Background(), msg)
	if err != nil {
		return nil, err
	}
	return publisher, nil
}
