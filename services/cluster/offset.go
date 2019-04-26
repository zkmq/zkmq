package cluster

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.etcd.io/etcd/clientv3/concurrency"

	"go.etcd.io/etcd/clientv3"

	"github.com/dynamicgo/xerrors"
)

func (cluster *clusterImpl) TopicOffset(topic string) (uint64, error) {
	resp, err := cluster.Etcd.Put(context.Background(), "topic/"+topic, topic, clientv3.WithPrevKV())

	if err != nil {
		return 0, xerrors.Wrapf(err, "get topic offset error")
	}

	if resp.PrevKv == nil {
		return 0, nil
	}

	return uint64(resp.PrevKv.Version), nil
}

func (cluster *clusterImpl) ConsumerOffset(topic string, consumer string) (uint64, error) {
	resp, err := cluster.Etcd.Get(context.Background(), fmt.Sprintf("consumer/%s/%s", topic, consumer))

	if err != nil {
		return 0, xerrors.Wrapf(err, "get topic offset error")
	}

	if resp.Count == 0 {
		return 0, nil
	}

	return toOffset(resp.Kvs[0].Value), nil
}

func (cluster *clusterImpl) UpdateConsumerOffset(topic string, consumer string, offset uint64) error {
	key := fmt.Sprintf("consumer/%s/%s", topic, consumer)

	_, err := concurrency.NewSTM(cluster.Etcd, func(stm concurrency.STM) error {

		val := stm.Get(key)

		if val == "" {
			if offset == 0 {
				stm.Put(key, string(fromOffset(1)))
			}

			return nil
		}

		if toOffset([]byte(val)) == offset {
			stm.Put(key, string(fromOffset(offset+1)))
		}

		return nil
	})

	if err != nil {
		return xerrors.Wrapf(err, "update topic %s consumer %s offset %d error", topic, consumer, offset)
	}

	return err
}

func toOffset(buff []byte) uint64 {
	return binary.BigEndian.Uint64(buff)
}

func fromOffset(offset uint64) []byte {

	var buff [8]byte

	binary.BigEndian.PutUint64(buff[:], offset)

	return buff[:]
}
