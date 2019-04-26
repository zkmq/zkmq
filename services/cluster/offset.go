package cluster

import (
	"context"
	"encoding/binary"

	"github.com/dynamicgo/xerrors"
)

func (cluster *clusterImpl) TopicOffset(topic string) (uint64, error) {

	if cluster.isMaster {
		return cluster.localTopicOffset(topic)
	}

	remote, err := cluster.tryGetMasterBroker()

	if err != nil {
		return 0, err
	}

	return remote.TopicOffset(topic)
}

func (cluster *clusterImpl) localTopicOffset(topic string) (uint64, error) {

	getResp, err := cluster.Etcd.Get(context.Background(), "topic/"+topic)

	if err != nil {
		return 0, xerrors.Wrapf(err, "get zkmq nodes error")
	}

	offset := uint64(0)

	if getResp.Count != 0 {
		offset = toOffset(getResp.Kvs[0].Value)
		offset++
	}

	_, err = cluster.Etcd.Put(context.Background(), "topic/"+topic, string(fromOffset(offset)))

	if err != nil {
		return 0, xerrors.Wrapf(err, "update topic %s offset %d error", topic, offset)
	}

	return offset, nil
}

func toOffset(buff []byte) uint64 {
	return binary.BigEndian.Uint64(buff)
}

func fromOffset(offset uint64) []byte {

	var buff [8]byte

	binary.BigEndian.PutUint64(buff[:], offset)

	return buff[:]
}
