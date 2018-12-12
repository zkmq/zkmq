package test

import (
	"testing"

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

	config := config.NewConfig()

	err := config.Load(file.NewSource(file.WithPath("../conf/broker.json")))

	if err != nil {
		panic(err)
	}

	if err := gomesh.Start(config); err != nil {
		panic(err)
	}
}

func TestPushMessage(t *testing.T) {

	conf := config.NewConfig()

	err := conf.Load(memory.NewSource(memory.WithData([]byte(`
	{
		"topic":"test",
		"producer":"hello",
		"remote":"127.0.0.1:2019"
	}
	`))))

	require.NoError(t, err)

	println(conf.Get("topic").String("`"))

	producer, err := client.NewProducer(conf)

	require.NoError(t, err)

	record, err := producer.Record([]byte("test"), []byte("hello world"))

	require.NoError(t, err)

	err = producer.Send(record)

	require.NoError(t, err)
}
