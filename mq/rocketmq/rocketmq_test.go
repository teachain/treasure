package rocketmq

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"
	"os/signal"
	"testing"
	"time"
)

func TestNewRocketMessageQueue(t *testing.T) {
	rlog.SetLogLevel("error")
	consumer := NewRocketMessageQueue(&Config{
		Addr:         []string{"192.168.4.33:9876", "192.168.4.33:9876"},
		Topic:        "rocketmqTest",
		GroupName:    "rocketmqGroup",
		Retries:      3,
		Tag:          "my",
		PullInterval: 0,
		AccessKey:    "123456",
		SecretKey:    "78909",
	})
	producer := NewRocketMessageQueue(&Config{
		Addr:         []string{"192.168.4.33:9876", "192.168.4.33:9876"},
		Topic:        "rocketmqTest",
		GroupName:    "rocketmqGroup",
		Retries:      3,
		Tag:          "my",
		PullInterval: 0,
		AccessKey:    "123456",
		SecretKey:    "78901",
	})

	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:

			}
			msgId, err := producer.Publish([]byte(time.Now().Format(time.DateTime)))
			if err != nil {
				fmt.Println(err.Error())
			} else {
				fmt.Println("send message id", msgId)
			}
			time.Sleep(time.Second)
		}

	}()

	go func() {
		err := consumer.StartConsumer(func(id string, msg []byte) error {
			fmt.Println("handle message", "id=", id, "msg=", string(msg))
			return nil
		})
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch
	close(stop)
	err := producer.Shutdown()
	if err != nil {
		t.Error(err.Error())
	}
	err = consumer.Shutdown()
	if err != nil {
		t.Error(err.Error())
	}

}
