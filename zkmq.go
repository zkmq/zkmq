package zkmq

import (
	"github.com/dynamicgo/xerrors/apierr"
)

//go:generate protoc  --go_out=plugins=grpc:. zkmq.proto

// errors
var (
	ErrExists = apierr.WithScope(-1, "node exists", "zkmq")
)

// Cluster cluster low layer service
type Cluster interface {
	Master() (BrokerClient, bool)
	IsMaster() bool
	GetTopicBrokers(topic string)
}
