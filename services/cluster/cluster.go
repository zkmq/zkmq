package cluster

import (
	"sync"
	"time"

	"github.com/dynamicgo/slf4go"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/xerrors"
	"github.com/zkmq/zkmq"
	"github.com/zkmq/zkmq/consistent"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type clusterImpl struct {
	sync.Mutex
	slf4go.Logger
	Etcd     *clientv3.Client             `inject:"zkmq.Etcd"` //
	nodeName string                       // cluster node name
	laddr    string                       // cluster node external listen address
	ttl      time.Duration                // etcd session ttl
	session  *concurrency.Session         // etcd session
	mutex    *concurrency.Mutex           // etcd mutex
	hashring *consistent.Consistent       // consistent hash range
	neighbor map[string]zkmq.BrokerClient // register neighbor nodes
}

// New .
func New(config config.Config) (zkmq.Cluster, error) {

	nodeName := config.Get("node", "name").String("")

	if nodeName == "" {
		return nil, xerrors.New("expect zkmq cluster node name")
	}

	laddr := config.Get("node", "laddr").String("")

	if laddr == "" {
		return nil, xerrors.New("expect zkmq cluster node laddr ")
	}

	return &clusterImpl{
		Logger:   slf4go.Get("cluster"),
		nodeName: nodeName,
		laddr:    laddr,
		ttl:      config.Get("ttl").Duration(5 * time.Second),
		hashring: consistent.New(config.Get("hashring", "vnodes").Int(20)),
	}, nil
}

func (cluster *clusterImpl) Start() error {
	return cluster.bootstrap()
}
