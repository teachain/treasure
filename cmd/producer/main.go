package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
	"strconv"
	"time"

	rocketmqApp "github.com/teachain/treasure/mq/rocketmq"
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
	conf := &MqConfig{
		Topic:     *topic,
		Name:      *groupId,
		Addr:      []string{*addr},
		Tag:       "anchor",
		AccessKey: *accessKey,
		SecretKey: *secretKey,
	}
	target := rocketmqApp.ResolveDomainNames(conf.Addr)
	credentials := primitive.Credentials{
		AccessKey: conf.AccessKey,
		SecretKey: conf.SecretKey}
	options := make([]producer.Option, 0)
	options = append(options, producer.WithInstanceName(conf.Name+"Producer"))
	options = append(options, producer.WithGroupName(conf.Name))
	options = append(options, producer.WithNameServer(target))
	options = append(options, producer.WithRetry(2))
	options = append(options, producer.WithCredentials(credentials))

	producerInstance, err := rocketmq.NewProducer(options...)
	if err != nil {
		fmt.Println("NewProducer error:", err.Error())
		return
	}
	if err = producerInstance.Start(); err != nil {
		fmt.Println("Start error:", err.Error())
		return
	}
	// ping pong
	msg := primitive.NewMessage(conf.Topic, []byte("startTime:"+time.Now().Format(time.DateTime)))
	msg.WithKeys([]string{"SIPCProducerStart:pid:" + strconv.Itoa(os.Getpid())})
	_, err = producerInstance.SendSync(context.Background(), msg)
	if err != nil {
		fmt.Println("SendSync error:", err.Error())
		return
	}
	for {
		value := []byte(time.Now().Format(time.DateTime) + " ping")

		msg := primitive.NewMessage(conf.Topic, value).WithTag(conf.Tag)
		result, err := producerInstance.SendSync(context.Background(), msg)
		if err != nil {
			fmt.Println("produce sendSync:", "err", err.Error())
			break
		}
		fmt.Println("msgId=", result.MsgID)
		time.Sleep(time.Second * 10)
	}

}
