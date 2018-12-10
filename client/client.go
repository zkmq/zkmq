package client

import (
	"github.com/dynamicgo/mq"
	"github.com/zkmq/zkmq"
)

// Consumer .
type Consumer interface {
	mq.Consumer
}

// Producer .
type Producer interface {
	mq.Producer
}

type recordWrapper struct {
	*zkmq.Record
}
