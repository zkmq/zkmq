package storage

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"sync"

	"github.com/dynamicgo/slf4go"

	"github.com/dynamicgo/xerrors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zkmq/zkmq"
)

type topicStorage struct {
	sync.Mutex                // mixin mutex
	slf4go.Logger             // mixin logger
	db            *leveldb.DB // db
	offset        uint64      // cached offset
	topic         string      // topic name
}

func (storage *storageLevelDB) newTopic(name string) (*topicStorage, error) {

	dbpath := filepath.Join(storage.rootpath, "storage", name)

	storage.DebugF("open topic %s storage path %s", name, dbpath)

	db, err := leveldb.OpenFile(dbpath, nil)

	if err != nil {
		return nil, xerrors.Wrapf(err, "open leveldb %s error", dbpath)
	}

	return &topicStorage{
		Logger: slf4go.Get("storage-topic"),
		db:     db,
		topic:  name,
	}, nil
}

func (topic *topicStorage) getOffset(offset uint64) (uint64, error) {
	topic.Lock()
	defer topic.Unlock()

	if topic.offset > offset {
		offset = topic.offset
	}

	for {
		key := topic.getKeyByOffset(offset)

		ok, err := topic.db.Has(key, nil)

		if err != nil {
			return 0, xerrors.Wrapf(err, "topic %s call db.Has error", topic.topic)
		}

		if !ok {
			break
		}

		offset++
	}

	topic.offset = offset + 1

	return offset, nil
}

func (topic *topicStorage) getKeyByOffset(offset uint64) []byte {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, offset)

	return buff
}

func (topic *topicStorage) writeRecord(offset uint64, record *zkmq.Record) (uint64, error) {

	offset, err := topic.getOffset(offset)

	if err != nil {
		return 0, err
	}

	buff, err := json.Marshal(record)

	if err != nil {
		return 0, xerrors.Wrapf(err, "marshal record %s error", hex.EncodeToString(record.Key))
	}

	err = topic.db.Put(topic.getKeyByOffset(offset), buff, nil)

	if err != nil {
		return 0, xerrors.Wrapf(err, "invoke topic %s leveldb put error", topic.topic)
	}

	return offset, nil
}

func (topic *topicStorage) readRecord(offset, count uint64) ([]*zkmq.Record, error) {

	var records []*zkmq.Record

	for i := uint64(0); i < count; i++ {
		record, err := topic.readOne(offset + i)

		if err == leveldb.ErrNotFound {
			break
		}

		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func (topic *topicStorage) readOne(offset uint64) (*zkmq.Record, error) {
	buff, err := topic.db.Get(topic.getKeyByOffset(offset), nil)

	if err == leveldb.ErrNotFound {
		return nil, err
	}

	if err != nil {
		return nil, xerrors.Wrapf(err, "invoke topic %s leveldb get error", topic.topic)
	}

	var record *zkmq.Record

	if err := json.Unmarshal(buff, &record); err != nil {
		return nil, xerrors.Wrapf(err, "unmarshal topic %s record %s error", topic.topic, string(buff))
	}

	return record, nil
}
