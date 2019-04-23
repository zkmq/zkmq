package storage

import (
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq"
)

type storageImpl struct {
	slf4go.Logger
	name    string
	Cluster zkmq.Cluster `inject:"zkmq.Cluster"`
}

// New .
func New(config config.Config) (zkmq.Storage, error) {
	return &storageImpl{
		Logger: slf4go.Get("storage"),
	}, nil
}

func (storage *storageImpl) Name() string {
	return storage.Cluster.Name()
}

func (storage *storageImpl) Write(record *zkmq.Record) error {
	storage.DebugF("write topic %s record %d", record.Topic, record.Offset)
	return nil
}
