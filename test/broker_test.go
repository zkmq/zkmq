package test

import (
	"gx/ipfs/QmRvYNctevGUW52urgmoFZscT6buMKqhHezLUS64WepGWn/go-net/context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zkmq/zkmq"
	"google.golang.org/grpc"

	extend "github.com/dynamicgo/go-config-extend"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/file"
	"github.com/dynamicgo/gomesh"
	_ "github.com/dynamicgo/gomesh/agent/basic"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq/services/broker"
	"github.com/zkmq/zkmq/services/cluster"
	"github.com/zkmq/zkmq/services/etcd"
	"github.com/zkmq/zkmq/services/storage"
)

var logger = slf4go.Get("test")
var client zkmq.GatewayClient

func init() {
	gomesh.LocalService("zkmq.Broker", func(config config.Config) (gomesh.Service, error) {
		return broker.New(config)
	})

	gomesh.LocalService("zkmq.Etcd", func(config config.Config) (gomesh.Service, error) {
		return etcd.New(config)
	})

	gomesh.LocalService("zkmq.Cluster", func(config config.Config) (gomesh.Service, error) {
		return cluster.New(config)
	})

	gomesh.LocalService("zkmq.Storage", func(config config.Config) (gomesh.Service, error) {
		return storage.New(config)
	})

	config := config.NewConfig()

	err := config.Load(file.NewSource(file.WithPath("../conf/broker.1.json")))

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

	conn, err := grpc.Dial("localhost:3018", grpc.WithInsecure())

	if err != nil {
		panic(err)
	}

	client = zkmq.NewGatewayClient(conn)
}

func TestPush(t *testing.T) {
	resp, err := client.Push(context.Background(), &zkmq.Record{
		Topic:   "test",
		Key:     []byte("test"),
		Content: []byte("test"),
	})

	require.NoError(t, err)

	println(resp.Offset)
}
