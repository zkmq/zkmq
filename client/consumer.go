package client

import (
	"context"
	"time"

	"github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	"github.com/dynamicgo/retry"
	"github.com/dynamicgo/slf4go"
	"github.com/dynamicgo/xerrors"
	"github.com/zkmq/zkmq"
	"google.golang.org/grpc"
)

func (record *recordWrapper) Key() []byte {
	return []byte(record.Record.Key)
}

func (record *recordWrapper) Value() []byte {
	return record.Record.Content
}

type consumerImpl struct {
	slf4go.Logger
	consumerID string
	topicID    string
	client     zkmq.BrokerClient
	listener   zkmq.Broker_ListenClient
	cacher     chan mq.Record
	errors     chan error
}

// NewConsumer .
func NewConsumer(config config.Config) (mq.Consumer, error) {

	conn, err := grpc.Dial(config.Get("remote").String("127.0.0.1:2018"), grpc.WithInsecure())

	if err != nil {
		return nil, xerrors.Wrapf(err, "dial remote zkmq error")
	}

	client := zkmq.NewBrokerClient(conn)

	topic := config.Get("topic").String("")

	if topic == "" {
		return nil, xerrors.Errorf("must set topic")
	}

	consumerID := config.Get("consumer").String("")

	if consumerID == "" {
		return nil, xerrors.Errorf("must set consumer")
	}

	listener, err := client.Listen(context.TODO(), &zkmq.Topic{
		Key: topic,
	})

	if err != nil {
		return nil, xerrors.Wrapf(err, "topic listener error")
	}

	consumer := &consumerImpl{
		Logger:     slf4go.Get("zkmq-consumer"),
		client:     client,
		listener:   listener,
		cacher:     make(chan mq.Record, config.Get("cached").Int(1)),
		errors:     make(chan error, 10),
		consumerID: consumerID,
		topicID:    topic,
	}

	go consumer.pullLoop()

	return consumer, nil
}

func (consumer *consumerImpl) pullLoop() {

	for {
		consumer.cacher <- consumer.pullOne()
	}
}

func (consumer *consumerImpl) pullOne() *recordWrapper {
	var record *recordWrapper

	action := retry.New(func(ctx context.Context) error {

		records, err := consumer.doPull()

		if err != nil {
			return err
		}

		record = &recordWrapper{
			Record: records[0],
		}

		return nil
	}, retry.Infinite(), retry.WithBackoff(time.Minute, 1))

	for {
		select {
		case <-action.Do():
			consumer.DebugF("consumer %s pull %s record %s", consumer.consumerID, consumer.topicID, record.GetOffset())
			return record
		case err := <-action.Error():
			consumer.errors <- err
		}
	}
}

func (consumer *consumerImpl) doPull() ([]*zkmq.Record, error) {

	resp, err := consumer.client.Pull(context.TODO(), &zkmq.PullRequest{
		Consumer: consumer.consumerID,
		Topic:    consumer.topicID,
		Count:    1,
	})

	if err != nil {
		return nil, xerrors.Wrapf(err, "consumer %s pull topic %s error", consumer.consumerID, consumer.topicID)
	}

	return resp.GetRecord(), nil
}

func (consumer *consumerImpl) Recv() <-chan mq.Record {
	return consumer.cacher
}

func (consumer *consumerImpl) Errors() <-chan error {
	return consumer.errors
}

func (consumer *consumerImpl) Commit(record mq.Record) error {

	offset := record.(*recordWrapper).GetOffset()

	_, err := consumer.client.Commit(context.TODO(), &zkmq.CommitRequest{
		Consumer: consumer.consumerID,
		Topic:    consumer.topicID,
		Offset:   offset,
	})

	if err != nil {
		return xerrors.Wrapf(err, "commit offset error")
	}

	return nil
}
