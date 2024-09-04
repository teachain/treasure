package rocketmq

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/teachain/treasure/mq"
)

type RocketMessageQueue struct {
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	config   *Config
}
type Config struct {
	Addr         []string //nameserver地址
	Topic        string   //主题
	GroupName    string   //组名
	Retries      int      //重试次数
	Tag          string   //标签
	PullInterval int      //数据拉取间隔
	AccessKey    string   //鉴权
	SecretKey    string   //鉴权
}

func NewRocketMessageQueue(config *Config) mq.MessageQueue {
	return &RocketMessageQueue{
		config: config,
	}
}
