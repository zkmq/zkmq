package broker

import (
	"context"

	"github.com/dynamicgo/xerrors"

	"github.com/dynamicgo/slf4go"

	config "github.com/dynamicgo/go-config"
	"github.com/zkmq/zkmq"
)

type brokerImpl struct {
	slf4go.Logger              // mixin logger
	Storage       zkmq.Storage `inject:"zkmq.Storage"` // inject storage
}

// New create new broker
func New(config config.Config) (zkmq.BrokerServer, error) {
	broker := &brokerImpl{}

	return broker, nil
}

func (broker *brokerImpl) Push(ctx context.Context, record *zkmq.Record) (*zkmq.PushResponse, error) {

	offset, err := broker.Storage.Write(record)

	if err != nil {
		return nil, xerrors.Wrapf(err, "record(%s) write to storage err", record.GetKey())
	}

	return &zkmq.PushResponse{
		Offset: offset,
	}, nil
}

func (broker *brokerImpl) Pull(ctx context.Context, req *zkmq.PullRequest) (*zkmq.PullResponse, error) {

	broker.Storage.Read(req.Topic, req.Offset, 1)

	return nil, nil
}

func (broker *brokerImpl) Commit(context.Context, *zkmq.CommitRequest) (*zkmq.CommitRespose, error) {
	return nil, nil
}

func (broker *brokerImpl) Listen(*zkmq.Topic, zkmq.Broker_ListenServer) error {
	return nil
}
