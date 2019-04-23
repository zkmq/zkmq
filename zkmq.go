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
	Name() string
	TopicOffset(topic string) (uint64, error)
	TopicStorage(topic string) ([]Storage, error)
}

// Storage .
type Storage interface {
	Name() string
	Write(record *Record) error
}
