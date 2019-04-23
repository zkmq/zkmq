package broker

import (
	"context"
	"sync"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq"
	"google.golang.org/grpc"
)

type brokerImpl struct {
	sync.RWMutex               // mixin read/writer mutex
	slf4go.Logger              // mixin
	Cluster       zkmq.Cluster `inject:"zkmq.Cluster"` //
	Storage       zkmq.Storage `inject:"zkmq.Storage"` //
	replicas      int          // write replicas
}

// New .
func New(config config.Config) (zkmq.BrokerServer, error) {

	return &brokerImpl{
		Logger:   slf4go.Get("zkmq-broker"),
		replicas: config.Get("write", "replicas").Int(2),
	}, nil
}

func (broker *brokerImpl) GrpcHandle(server *grpc.Server) error {
	zkmq.RegisterGatewayServer(server, broker)
	zkmq.RegisterBrokerServer(server, broker)
	return nil
}

func (broker *brokerImpl) BrokerWrite(ctx context.Context, record *zkmq.Record) (*zkmq.PushResponse, error) {
	if err := broker.Storage.Write(record); err != nil {
		return nil, err
	}

	return &zkmq.PushResponse{
		Offset: record.Offset,
	}, nil
}
