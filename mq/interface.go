package mq

type MessageQueue interface {
	Publish(body []byte) (string, error)
	StartConsumer(handler MsgHandler) error
	Shutdown() error
}

// MsgHandler id 消费者需要通过此Id来判断该消息是否已被消费
type MsgHandler func(id string, msg []byte) error
