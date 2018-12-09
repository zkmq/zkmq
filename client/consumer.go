package client

import (
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
)

type consumerImpl struct {
}

// NewConsumer .
func NewConsumer(config config.Config) (mq.Consumer, error) {
	return nil, nil
}
