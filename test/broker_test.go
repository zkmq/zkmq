package test

import (
	"testing"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/file"
	"github.com/dynamicgo/gomesh"
	_ "github.com/dynamicgo/gomesh/agent/basic"
	"github.com/dynamicgo/slf4go"
	"github.com/zkmq/zkmq"
	"github.com/zkmq/zkmq/services/broker"
	"github.com/zkmq/zkmq/services/storage"
	"google.golang.org/grpc"
)

var logger = slf4go.Get("test")
var brokerClient zkmq.BrokerClient

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

	conn, err := grpc.Dial("127.0.0.1:2019", grpc.WithInsecure())

	if err != nil {
		panic(err)
	}

	brokerClient = zkmq.NewBrokerClient(conn)
}

func TestPushMessage(t *testing.T) {

}
