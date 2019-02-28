package client

import (
	"context"
	"time"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
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
	consumerID   string
	topicID      string
	client       zkmq.BrokerClient
	listener     zkmq.Broker_ListenClient
	cacher       chan mq.Record
	errors       chan error
	notifyChan   chan uint64
	lastRecord   *zkmq.Record
	retryTimeout time.Duration
}

// NewConsumer .
func NewConsumer(config config.Config) (Consumer, error) {

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

	listener, err := client.Listen(context.Background(), &zkmq.Topic{
		Key: topic,
	})

	if err != nil {
		return nil, xerrors.Wrapf(err, "topic listener error")
	}

	consumer := &consumerImpl{
		Logger:       slf4go.Get("zkmq-consumer"),
		client:       client,
		listener:     listener,
		cacher:       make(chan mq.Record, config.Get("cached").Int(1)),
		errors:       make(chan error, 10),
		notifyChan:   make(chan uint64, 100),
		consumerID:   consumerID,
		topicID:      topic,
		retryTimeout: config.Get("backoff").Duration(time.Second * 30),
	}

	go consumer.listenLoop()

	go consumer.pullLoop()

	return consumer, nil
}

func (consumer *consumerImpl) pullLoop() {

	ticker := time.NewTicker(consumer.retryTimeout)
	defer ticker.Stop()

	// consumer.listener.Recv()

	for {

		record, err := consumer.pullOne()

		if err == nil {
			consumer.cacher <- record
			continue
		}

		select {
		case <-ticker.C:
		case <-consumer.notifyChan:
		}
	}
}

func (consumer *consumerImpl) listenLoop() {
	for {
		changed, err := consumer.listener.Recv()

		if err != nil {
			consumer.errors <- err
			time.Sleep(consumer.retryTimeout)
			continue
		}

		consumer.notifyChan <- changed.Offset
	}

}

func (consumer *consumerImpl) pullOne() (*recordWrapper, error) {
	var record *recordWrapper

	records, err := consumer.doPull()

	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, xerrors.New("empty consumer queue")
	}

	record = &recordWrapper{
		Record: records[0],
	}

	consumer.lastRecord = records[0]

	return record, nil
}

func (consumer *consumerImpl) doPull() ([]*zkmq.Record, error) {

	offset := uint64(0)

	if consumer.lastRecord != nil {
		offset = consumer.lastRecord.Offset + 1
	}

	consumer.DebugF("consumer %s pull topic %s with offset %d", consumer.consumerID, consumer.topicID, offset)

	resp, err := consumer.client.Pull(context.TODO(), &zkmq.PullRequest{
		Consumer: consumer.consumerID,
		Topic:    consumer.topicID,
		Count:    1,
		Offset:   offset,
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
