package cluster

import (
	"context"

	"google.golang.org/grpc"

	"github.com/dynamicgo/xerrors"

	"github.com/zkmq/zkmq"
)

type remoteStorageImpl struct {
	name   string
	remote string
	client zkmq.BrokerClient
}

func newRemoteStorage(name string, remote string) (zkmq.Storage, error) {

	conn, err := grpc.Dial(remote, grpc.WithInsecure())

	if err != nil {
		return nil, xerrors.Wrapf(err, "dial to %s error", remote)
	}

	client := zkmq.NewBrokerClient(conn)

	return &remoteStorageImpl{
		client: client,
		name:   name,
		remote: remote,
	}, nil
}

func (remote *remoteStorageImpl) Name() string {
	return remote.name
}
func (remote *remoteStorageImpl) Write(record *zkmq.Record) error {
	_, err := remote.client.BrokerWrite(context.Background(), record)

	if err != nil {
		return xerrors.Wrapf(err, "call remote storage %s %s err", remote.name, remote.remote)
	}

	return err
}
