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
	//对域名进行解析
	addresses := ResolveDomainNames(config.Addr)
	nameServer, err := primitive.NewNamesrvAddr(addresses...)
	if err != nil {
		return nil, err
	}
	options := make([]producer.Option, 0)
	options = append(options, producer.WithInstanceName(config.GroupName+"Producer"))
	options = append(options, producer.WithGroupName(config.GroupName))
	options = append(options, producer.WithNameServer(nameServer))
	options = append(options, producer.WithRetry(config.Retries))
	if len(config.AccessKey) > 0 && len(config.SecretKey) > 0 {
		credentials := primitive.Credentials{
			AccessKey: config.AccessKey,
			SecretKey: config.SecretKey,
		}
		options = append(options, producer.WithCredentials(credentials))
	}
	publisher, err := rocketmq.NewProducer(options...)
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
