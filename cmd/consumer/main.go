package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	rocketmqApp "github.com/teachain/treasure/mq/rocketmq"
	"os"
	"os/signal"
	"time"
)

var addr *string = flag.String("addr", "192.168.4.33:9876", "--addr=ip:port")

var topic *string = flag.String("topic", "da_local", "--topic=topicName")

var groupId *string = flag.String("groupId", "da_local_group", "--groupId=groupName")

var accessKey *string = flag.String("accessKey", "", "--accessKey=accessKey")

var secretKey *string = flag.String("secretKey", "", "--secretKey=secretKey")

type MqConfig struct {
	Addr            []string `yaml:"addr"`
	Topic           string   `yaml:"topic"`
	Tag             string   `yaml:"tag"`
	Name            string   `yaml:"name"`
	ConsumeInterval int      `yaml:"consumeInterval"`
	ConsumerSum     int      `yaml:"consumerSum"`
	LogLevel        string   `yaml:"logLevel"`
	AccessKey       string   `yaml:"accessKey"`
	SecretKey       string   `yaml:"secretKey"`
}

func main() {
	//必须先解析flag
	flag.Parse()
	rlog.SetLogLevel("error")
	conf := &MqConfig{
		Topic:     *topic,
		Name:      *groupId,
		Addr:      []string{*addr},
		Tag:       "anchor",
		AccessKey: *accessKey,
		SecretKey: *secretKey,
	}
	target := rocketmqApp.ResolveDomainNames(conf.Addr)

	options := make([]consumer.Option, 0)

	credentials := primitive.Credentials{
		AccessKey: conf.AccessKey,
		SecretKey: conf.SecretKey}

	options = append(options, consumer.WithCredentials(credentials))
	options = append(options, consumer.WithNameServer(target))
	options = append(options, consumer.WithInstance(fmt.Sprintf("%sConsumer-anchor-%d", conf.Name, 1)))
	options = append(options, consumer.WithGroupName(conf.Name))
	options = append(options, consumer.WithPullInterval(time.Duration(conf.ConsumeInterval)*1000*time.Millisecond))
	options = append(options, consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
	options = append(options, consumer.WithConsumerModel(consumer.Clustering))
	options = append(options, consumer.WithConsumeMessageBatchMaxSize(1))
	options = append(options, consumer.WithMaxReconsumeTimes(60))

	consumerObject, err := rocketmq.NewPushConsumer(options...)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := consumerObject.Subscribe(conf.Topic, consumer.MessageSelector{Type: consumer.TAG, Expression: conf.Tag}, subAnchorMsg); err != nil {
		fmt.Println(err.Error())
		return
	}

	//开启消费者
	if err := consumerObject.Start(); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("Waiting for receiving message")
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Kill, os.Interrupt)
	<-ch
	err = consumerObject.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s", err.Error())
	}
}

func subAnchorMsg(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range messages {
		fmt.Println("msgId", msg.MsgId, "body", string(msg.Body))
	}
	return consumer.ConsumeSuccess, nil
}
