package metadata

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"path/filepath"

	"github.com/dynamicgo/slf4go"
	"github.com/syndtr/goleveldb/leveldb"

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

	if err != nil {
		return 0, xerrors.Wrapf(err, "topic(%s) consumer(%s) read metadata error", topic, consumer)
	}

	offset := metadata.BytesToOffset(buff)

	return offset, nil
}

func (metadata *metadataSqlite) CommitOffset(topic, consumer string, offset uint64) (uint64, error) {

	trans, err := metadata.db.OpenTransaction()

	if err != nil {
		return 0, xerrors.Wrapf(err, "consumer %s open topic %s metadata error", consumer, topic)
	}

	buff, err := trans.Get([]byte(topic+consumer), nil)

	originOffset := uint64(0)

	if err != nil {
		if err != leveldb.ErrNotFound {
			trans.Discard()
			return 0, xerrors.Wrapf(err, "open topic %s metadata error", topic)
		}

		originOffset = 0
	} else {
		originOffset = metadata.BytesToOffset(buff)
	}

	if originOffset >= offset {
		metadata.WarnF("consumer(%s) topic(%s) discard commit offset %d, current is %d", consumer, topic, offset, originOffset)
		trans.Discard()
		return offset, nil
	}

	metadata.DebugF("consumer(%s) topic(%s) commit offset %d", consumer, topic, offset)

	err = trans.Put([]byte(topic+consumer), metadata.OffsetToBytes(offset), &opt.WriteOptions{Sync: true})

	if err != nil {
		trans.Discard()
		return 0, xerrors.Wrapf(err, "consumer %s write topic %s offset %d metadata error", consumer, topic, offset)
	}

	if err := trans.Commit(); err != nil {
		return 0, xerrors.Wrapf(err, "consumer %s write topic %s offset %d metadata error", consumer, topic, offset)
	}

	metadata.DebugF("consumer(%s) topic(%s) commit offset %d -- success", consumer, topic, offset)

	return offset, nil
}

func (metadata *metadataSqlite) CommitTopicHeader(topic string, offset uint64) (uint64, error) {

	trans, err := metadata.db.OpenTransaction()

	if err != nil {
		return 0, xerrors.Wrapf(err, "open topic %s metadata error", topic)
	}

	buff, err := trans.Get([]byte(topic), nil)

	originOffset := uint64(0)

	if err != nil {
		if err != leveldb.ErrNotFound {
			trans.Discard()
			return 0, xerrors.Wrapf(err, "open topic %s metadata error", topic)
		}

		originOffset = 0
	} else {
		originOffset = metadata.BytesToOffset(buff)
	}

	if originOffset >= offset {
		trans.Discard()
		return offset, nil
	}

	err = trans.Put([]byte(topic), metadata.OffsetToBytes(offset), nil)

	if err != nil {
		trans.Discard()
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
