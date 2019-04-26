package cluster

import (
	"context"

	"google.golang.org/grpc"

	"github.com/dynamicgo/xerrors"

	"github.com/zkmq/zkmq"
)

type remoteBroker struct {
	name   string
	remote string
	client zkmq.BrokerClient
}

func newRemoteBroker(name string, remote string) (*remoteBroker, error) {

	conn, err := grpc.Dial(remote, grpc.WithInsecure())

	if err != nil {
		return nil, xerrors.Wrapf(err, "dial to %s error", remote)
	}

	client := zkmq.NewBrokerClient(conn)

	return &remoteBroker{
		client: client,
		name:   name,
		remote: remote,
	}, nil
}

func (remote *remoteBroker) Name() string {
	return remote.name
}

func (remote *remoteBroker) Write(record *zkmq.Record) error {
	_, err := remote.client.BrokerWrite(context.Background(), record)

	if err != nil {
		return xerrors.Wrapf(err, "call remote storage %s %s err", remote.name, remote.remote)
	}

	return err
}

func (remote *remoteBroker) TopicOffset(topic string) (uint64, error) {
	return 0, nil
}
