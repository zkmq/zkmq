package broker

import (
	"context"
	"sync"

	"github.com/dynamicgo/xerrors"
	"google.golang.org/grpc"

	"github.com/dynamicgo/slf4go"

	config "github.com/dynamicgo/go-config"
	"github.com/zkmq/zkmq"
)

type brokerImpl struct {
	sync.RWMutex
	slf4go.Logger              // mixin logger
	Storage       zkmq.Storage `inject:"zkmq.Storage"` // inject storage
	listener      map[string][]zkmq.Broker_ListenServer
}

// New create new broker
func New(config config.Config) (zkmq.BrokerServer, error) {
	broker := &brokerImpl{
		Logger:   slf4go.Get("broker"),
		listener: make(map[string][]zkmq.Broker_ListenServer),
	}

	return broker, nil
}

func (broker *brokerImpl) BatchPush(ctx context.Context, req *zkmq.BatchPushRequest) (resp *zkmq.PushResponse, err error) {

	for _, record := range req.GetRecord() {
		resp, err = broker.Push(ctx, record)

		if err != nil {
			return
		}
	}

	return
}

func (broker *brokerImpl) Push(ctx context.Context, record *zkmq.Record) (*zkmq.PushResponse, error) {

	offset, err := broker.Storage.Write(record)

	if err != nil {
		return nil, xerrors.Wrapf(err, "record(%s) write to storage err", record.GetKey())
	}

	broker.DebugF("topic(%s) push record(%d) -- success", record.Topic, offset)

	broker.notifyConsumer(record.Topic, offset)

	return &zkmq.PushResponse{
		Offset: offset,
	}, nil
}

func (broker *brokerImpl) Pull(ctx context.Context, req *zkmq.PullRequest) (*zkmq.PullResponse, error) {

	records, err := broker.Storage.Read(req.Topic, req.Consumer, req.Count)

	if err != nil {
		return nil, xerrors.Wrapf(err, "read record err")
	}

	return &zkmq.PullResponse{
		Record: records,
	}, nil

}

func (broker *brokerImpl) Commit(ctx context.Context, req *zkmq.CommitRequest) (*zkmq.CommitRespose, error) {

	offset, err := broker.Storage.CommitOffset(req.Topic, req.Consumer, req.Offset)

	if err != nil {
		return nil, xerrors.Wrapf(err, "read record err")
	}

	return &zkmq.CommitRespose{
		Offset: offset,
	}, nil
}

func (broker *brokerImpl) Listen(topic *zkmq.Topic, listener zkmq.Broker_ListenServer) error {
	broker.Lock()
	defer broker.Unlock()

	broker.DebugF("append %s listener %p", topic.Key, listener)

	broker.listener[topic.GetKey()] = append(broker.listener[topic.Key], listener)

	return nil
}

func (broker *brokerImpl) notifyConsumer(topic string, offset uint64) {

	remained := broker.doNotifyConsumer(topic, offset)

	broker.Lock()
	defer broker.Unlock()

	broker.listener[topic] = remained
}

func (broker *brokerImpl) doNotifyConsumer(topic string, offset uint64) []zkmq.Broker_ListenServer {
	broker.RLock()
	defer broker.RUnlock()

	offsetChanged := &zkmq.OffsetChanged{
		Topic:  topic,
		Offset: offset,
	}

	listeners := broker.listener[topic]

	var remained []zkmq.Broker_ListenServer

	for _, l := range listeners {
		if err := l.Send(offsetChanged); err != nil {
			broker.DebugF("notify %s listener %p for err %s", topic, l, err)
			continue
		}

		remained = append(remained, l)
	}

	return remained
}

func (broker *brokerImpl) GrpcHandle(server *grpc.Server) error {
	zkmq.RegisterBrokerServer(server, broker)
	broker.InfoF("register grpc service for zkmq.Brokder")
	return nil
}
