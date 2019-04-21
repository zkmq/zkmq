package broker

import (
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
	zkmq.RegisterBrokerServer(server, broker)
	return nil
}
