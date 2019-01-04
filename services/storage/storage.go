package storage

import (
	"sync"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq"
)

type storageLevelDB struct {
	sync.RWMutex                           // mixin
	slf4go.Logger                          // mixin logger
	Metadata      zkmq.Metadata            `inject:"zkmq.Metadata"` // inejct metadata
	rootpath      string                   // the storage root directory path
	topic         map[string]*topicStorage // topic map
}

// New .
func New(config config.Config) (zkmq.Storage, error) {
	return &storageLevelDB{
		Logger:   slf4go.Get("storage"),
		rootpath: config.Get("path").String("/var/zkmq/"),
		topic:    make(map[string]*topicStorage),
	}, nil
}

func (storage *storageLevelDB) getTopic(name string) (*topicStorage, error) {

	storage.RLock()
	topic, ok := storage.topic[name]
	storage.RUnlock()

	if ok {
		return topic, nil
	}

	storage.Lock()
	defer storage.Unlock()

	topic, ok = storage.topic[name]

	if ok {
		return topic, nil
	}

	topic, err := storage.newTopic(name)

	if err != nil {
		return nil, err
	}

	storage.topic[name] = topic

	return topic, nil
}

func (storage *storageLevelDB) Write(record *zkmq.Record) (uint64, error) {

	topic, err := storage.getTopic(record.Topic)

	if err != nil {
		return 0, err
	}

	offset, err := storage.Metadata.TopicHeader(record.Topic)

	if err != nil {
		return 0, err
	}

	offset, err = topic.writeRecord(offset, record)

	if err != nil {
		return 0, err
	}

	_, err = storage.Metadata.CommitTopicHeader(record.Topic, offset+1)

	return offset, err
}

func (storage *storageLevelDB) Offset(topic string) uint64 {
	offset, err := storage.Metadata.TopicHeader(topic)

	if err != nil {
		storage.ErrorF("get topic %s header err: %s", topic, err)
		return 0
	}

	return offset
}

func (storage *storageLevelDB) ConsumerOffset(topic string, consumer string) uint64 {
	offset, err := storage.Metadata.ConsumerOffset(topic, consumer)

	if err != nil {
		storage.ErrorF("get topic %s header err: %s", topic, err)
		return 0
	}

	return offset
}

func (storage *storageLevelDB) CommitOffset(topic, consumer string, offset uint64) (uint64, error) {
	storage.DebugF("commit consumer(%s) topic(%s) offset %d", consumer, topic, offset+1)
	return storage.Metadata.CommitOffset(topic, consumer, offset+1)
}

func (storage *storageLevelDB) Read(topic string, consumer string, applyoffset uint64, number uint64) ([]*zkmq.Record, error) {

	topicStorage, err := storage.getTopic(topic)

	if err != nil {
		return nil, err
	}

	offset, err := storage.Metadata.ConsumerOffset(topic, consumer)

	if err != nil {
		return nil, err
	}

	if applyoffset > offset {
		offset = applyoffset
	}

	storage.DebugF("consumer(%s) topic(%s) read record from %d to %d", topic, consumer, offset, offset+number)

	return topicStorage.readRecord(offset, number)
}
