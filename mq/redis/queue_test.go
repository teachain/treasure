package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
	"testing"
	"time"
)

type MsgRequest struct {
	Msg  string `json:"msg"`
	Age  int    `json:"age"`
	Time string `json:"time"`
}

func TestNewDistributeQueue(t *testing.T) {
	opt := &redis.Options{
		Network:  "tcp",
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	}
	client := redis.NewClient(opt)

	// 此处为了测试 Redis 的连通性
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	messageQueue := NewMessageQueue(client, &Config{
		MaxLen:    200000,
		Count:     100,
		Timeout:   15,
		BatchSize: 20,
		Topic:     "hello2",
		GroupName: "hero",
	})
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			msgRequest := &MsgRequest{
				Msg:  "hello",
				Age:  18,
				Time: time.Now().Format("2006-01-02 15:04:05"),
			}
			data, err := json.Marshal(msgRequest)

			fmt.Println("data=", string(data))
			//BatchSize int64
			id, err := messageQueue.Publish(data)
			if err != nil {
				panic(err.Error())
			}
			fmt.Println("send message id=", id)
			time.Sleep(time.Second * 2)

		}

	}()

	go func() {
		err := messageQueue.StartConsumer(func(id string, msg []byte) error {
			fmt.Println("handle message id=", id, "msg=", string(msg))
			return nil
		})
		if err != nil {
			panic(err.Error())
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch
	close(stop)
	err = messageQueue.Shutdown()
	if err != nil {
		t.Error(err.Error())
	}

}

func TestMessageQueue_Publish(t *testing.T) {
	opt := &redis.Options{
		Network:  "tcp",
		Addr:     "192.168.4.146:36379",
		Password: "H6X2B@FyTouzgG,ek5Y5",
		DB:       5,
	}
	client := redis.NewClient(opt)

	// 此处为了测试 Redis 的连通性
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	messageQueue := NewMessageQueue(client, &Config{
		MaxLen:    200000,
		Count:     100,
		Timeout:   15,
		BatchSize: 20,
		Topic:     "hello2",
		GroupName: "hero",
	})
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			msgRequest := &MsgRequest{
				Msg: "hello",
				Age: 18,
			}
			data, err := json.Marshal(msgRequest)
			//BatchSize int64
			id, err := messageQueue.Publish(data)
			if err != nil {
				panic(err.Error())
			}
			fmt.Println("send message id=", id)
			time.Sleep(time.Second * 2)

		}

	}()

	go func() {
		err := messageQueue.StartConsumer(func(id string, msg []byte) error {
			fmt.Println("handle message id=", id, "msg=", string(msg))
			return nil
		})
		if err != nil {
			panic(err.Error())
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch
	close(stop)
	err = messageQueue.Shutdown()
	if err != nil {
		t.Error(err.Error())
	}
}
