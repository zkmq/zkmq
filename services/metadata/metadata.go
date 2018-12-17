package metadata

import (
	"encoding/binary"
	"gx/ipfs/QmbBhyDKsY4mbY6xsKt3qu9Y7FPvMJ6qbD8AMjYYvPRw1g/goleveldb/leveldb"
	"path/filepath"

	"github.com/dynamicgo/slf4go"

	"github.com/dynamicgo/xerrors"

	config "github.com/dynamicgo/go-config"
	"github.com/zkmq/zkmq"
)

type metadataSqlite struct {
	slf4go.Logger
	db *leveldb.DB
}

// New .
func New(config config.Config) (zkmq.Metadata, error) {

	dbpath := filepath.Join(config.Get("path").String("/var/zkmq"), "metadata")

	db, err := leveldb.OpenFile(dbpath, nil)

	if err != nil {
		return nil, xerrors.Wrapf(err, "open zkmq metdata db error")
	}

	return &metadataSqlite{
		Logger: slf4go.Get("metadata"),
		db:     db,
	}, nil
}

func (metadata *metadataSqlite) OffsetToBytes(offset uint64) []byte {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, offset)

	return buff
}

func (metadata *metadataSqlite) BytesToOffset(offset []byte) uint64 {

	return binary.BigEndian.Uint64(offset)

}

func (metadata *metadataSqlite) ConsumerOffset(topic string, consumer string) (uint64, error) {

	buff, err := metadata.db.Get([]byte(topic+consumer), nil)

	if err == leveldb.ErrNotFound {
		return 0, nil
	}

	offset := metadata.BytesToOffset(buff)

	return offset, nil
}

func (metadata *metadataSqlite) CommitOffset(topic, consumer string, offset uint64) (uint64, error) {

	trans, err := metadata.db.OpenTransaction()

	if err != nil {
		return 0, xerrors.Wrapf(err, "consumer %s open topic %s metadata error", consumer, topic)
	}

	err = trans.Put([]byte(topic+consumer), metadata.OffsetToBytes(offset), nil)

	if err != nil {
		return 0, xerrors.Wrapf(err, "consumer %s write topic %s offset %d metadata error", consumer, topic, offset)
	}

	if err := trans.Commit(); err != nil {
		return 0, xerrors.Wrapf(err, "consumer %s write topic %s offset %d metadata error", consumer, topic, offset)
	}

	return offset, nil
}

func (metadata *metadataSqlite) CommitTopicHeader(topic string, offset uint64) (uint64, error) {

	trans, err := metadata.db.OpenTransaction()

	if err != nil {
		return 0, xerrors.Wrapf(err, "open topic %s metadata error", topic)
	}

	err = trans.Put([]byte(topic), metadata.OffsetToBytes(offset), nil)

	if err != nil {
		return 0, xerrors.Wrapf(err, "write topic %s offset %d metadata error", topic, offset)
	}

	if err := trans.Commit(); err != nil {
		return 0, xerrors.Wrapf(err, "write topic %s offset %d metadata error", topic, offset)
	}

	return offset, nil
}

func (metadata *metadataSqlite) TopicHeader(topic string) (uint64, error) {

	buff, err := metadata.db.Get([]byte(topic), nil)

	if err == leveldb.ErrNotFound {
		return 0, nil
	}

	offset := metadata.BytesToOffset(buff)

	return offset, nil
}
