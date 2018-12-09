package client

import (
	"github.com/dynamicgo/mq"
)

// Consumer .
type Consumer interface {
	// Recv get recv record chan
	Recv() <-chan mq.Record
	// Errors get errors chan
	Errors() <-chan error
	// Commit commit record offset
	Commit(record mq.Record) error
}

// Producer .
type Producer interface {
	Record(key []byte, value []byte) (mq.Record, error)
	Send(record mq.Record) error
	Batch(record []mq.Record) error
}
