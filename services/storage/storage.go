package storage

import (
	config "github.com/dynamicgo/go-config"
	"github.com/zkmq/zkmq"
)

type storageLevelDB struct {
}

// New .
func New(config config.Config) (zkmq.Storage, error) {
	return &storageLevelDB{}, nil
}

func (storage *storageLevelDB) Write(record *zkmq.Record) (uint64, error) {
	return 0, nil
}
func (storage *storageLevelDB) Offset(topic string) uint64 {
	return 0
}
func (storage *storageLevelDB) ConsumerOffset(topic string, consumer string) uint64 {
	return 0
}
func (storage *storageLevelDB) CommitOffset(topic, consumer string, offset uint64) uint64 {
	return 0
}
func (storage *storageLevelDB) Read(topic string, offset uint64, number uint64) ([]*zkmq.Record, error) {
	return nil, nil
}
