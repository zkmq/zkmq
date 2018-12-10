package client

import (
	"gx/ipfs/QmRvYNctevGUW52urgmoFZscT6buMKqhHezLUS64WepGWn/go-net/context"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	"github.com/dynamicgo/slf4go"
	"github.com/dynamicgo/xerrors"
	"github.com/zkmq/zkmq"
	"google.golang.org/grpc"
)

type producerImpl struct {
	slf4go.Logger
	producerID string
	topicID    string
	client     zkmq.BrokerClient
}

// NewProducer .
func NewProducer(config config.Config) (mq.Producer, error) {
	conn, err := grpc.Dial(config.Get("remote").String("127.0.0.1:2018"), grpc.WithInsecure())

	if err != nil {
		return nil, xerrors.Wrapf(err, "dial remote zkmq error")
	}

	client := zkmq.NewBrokerClient(conn)

	topic := config.Get("topic").String("")

	if topic == "" {
		return nil, xerrors.Errorf("must set topic")
	}

	producerID := config.Get("producer").String("")

	if producerID == "" {
		return nil, xerrors.Errorf("must set producer")
	}

	return &producerImpl{
		Logger:     slf4go.Get("zkmq-producer"),
		producerID: producerID,
		topicID:    topic,
		client:     client,
	}, nil
}

func (producer *producerImpl) Record(key []byte, value []byte) (mq.Record, error) {
	zkR := &zkmq.Record{
		Key:     key,
		Content: value,
	}

	return &recordWrapper{
		Record: zkR,
	}, nil
}
func (producer *producerImpl) Send(record mq.Record) error {

	r := record.(*recordWrapper).Record

	_, err := producer.client.Push(context.TODO(), r)

	if err != nil {
		return xerrors.Wrapf(err, "producer %s push %s record error", producer.producerID, producer.topicID)
	}

	return nil
}
func (producer *producerImpl) Batch(record []mq.Record) error {

	var rr []*zkmq.Record

	for _, r := range record {
		rr = append(rr, r.(*recordWrapper).Record)
	}

	_, err := producer.client.BatchPush(context.TODO(), &zkmq.BatchPushRequest{
		Record: rr,
	})

	if err != nil {
		return xerrors.Wrapf(err, "producer %s push %s record error", producer.producerID, producer.topicID)
	}

	return nil
}
