package test

import (
	"testing"
	"time"

	extend "github.com/dynamicgo/go-config-extend"

	"github.com/zkmq/zkmq/services/metadata"

	"github.com/dynamicgo/go-config/source/memory"

	"github.com/stretchr/testify/require"

	"github.com/zkmq/zkmq/client"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/file"
	"github.com/dynamicgo/gomesh"
	_ "github.com/dynamicgo/gomesh/agent/basic"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq/services/broker"
	"github.com/zkmq/zkmq/services/storage"
)

var logger = slf4go.Get("test")

func init() {
	gomesh.LocalService("zkmq.Broker", func(config config.Config) (gomesh.Service, error) {
		return broker.New(config)
	})

	gomesh.LocalService("zkmq.Storage", func(config config.Config) (gomesh.Service, error) {
		return storage.New(config)
	})

	gomesh.LocalService("zkmq.Metadata", func(config config.Config) (gomesh.Service, error) {
		return metadata.New(config)
	})

	config := config.NewConfig()

	err := config.Load(file.NewSource(file.WithPath("../conf/broker.json")))

	if err != nil {
		panic(err)
	}

	subconf, err := extend.SubConfig(config, "slf4go")

	if err != nil {
		panic(err)
	}

	if err := slf4go.Load(subconf); err != nil {
		panic(err)
	}

	if err := gomesh.Start(config); err != nil {
		panic(err)
	}

	createConsumerProducer()
}

var consumer client.Consumer
var producer client.Producer

func createConsumerProducer() {
	conf := config.NewConfig()

	err := conf.Load(memory.NewSource(memory.WithData([]byte(`
	{
		"topic":"test",
		"producer":"hello",
		"consumer":"wolrd",
		"remote":"127.0.0.1:2019",
		"default":{
			"backend":"null"
		}
	}
	`))))

	if err != nil {
		panic(err)
	}

	producer, err = client.NewProducer(conf)

	if err != nil {
		panic(err)
	}

	consumer, err = client.NewConsumer(conf)

	if err != nil {
		panic(err)
	}
}

func TestPushMessage(t *testing.T) {

	record, err := producer.Record([]byte("test"), []byte("hello world"))

	require.NoError(t, err)

	err = producer.Send(record)

	require.NoError(t, err)

	r := <-consumer.Recv()

	err = consumer.Commit(r)

	require.NoError(t, err)

	time.Sleep(time.Second * 1)
}

func BenchmarkProducer(t *testing.B) {
	for i := 0; i < t.N; i++ {
		record, err := producer.Record([]byte("test"), []byte("hello world"))

		require.NoError(t, err)

		err = producer.Send(record)

		require.NoError(t, err)

		r := <-consumer.Recv()

		err = consumer.Commit(r)

		require.NoError(t, err)
	}
}

