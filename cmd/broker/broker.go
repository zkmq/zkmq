package main

import (
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/gomesh"
	_ "github.com/dynamicgo/gomesh/agent/basic"
	"github.com/dynamicgo/gomesh/app"
	_ "github.com/dynamicgo/slf4go-aliyun"
	"github.com/zkmq/zkmq/services/broker"
	"github.com/zkmq/zkmq/services/metadata"
	"github.com/zkmq/zkmq/services/storage"
)

func main() {
	gomesh.LocalService("zkmq.Broker", func(config config.Config) (gomesh.Service, error) {
		return broker.New(config)
	})

	gomesh.LocalService("zkmq.Storage", func(config config.Config) (gomesh.Service, error) {
		return storage.New(config)
	})

	gomesh.LocalService("zkmq.Metadata", func(config config.Config) (gomesh.Service, error) {
		return metadata.New(config)
	})

	app.Run("zkmq-broker")
}
