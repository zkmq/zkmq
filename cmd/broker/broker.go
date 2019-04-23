package main

import (
	config "github.com/dynamicgo/go-config"
	_ "github.com/dynamicgo/slf4go-aliyun"
	_ "github.com/gomeshnetwork/agent/basic"
	"github.com/gomeshnetwork/gomesh"
	"github.com/gomeshnetwork/gomesh/app"
	"github.com/zkmq/zkmq/services/broker"
	"github.com/zkmq/zkmq/services/cluster"
	"github.com/zkmq/zkmq/services/etcd"
	"github.com/zkmq/zkmq/services/storage"
)

func main() {
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

	app.Run("zkmq-broker")

}
