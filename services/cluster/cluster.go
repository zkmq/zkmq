package cluster

import (
	"fmt"
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
	sync.RWMutex
	slf4go.Logger
	Etcd       *clientv3.Client        `inject:"zkmq.Etcd"`    //
	Storage    zkmq.Storage            `inject:"zkmq.Storage"` // topic storage
	nodeName   string                  // cluster node name
	laddr      string                  // cluster node external listen address
	ttl        time.Duration           // etcd session ttl
	replicas   int                     // mq write replicas
	session    *concurrency.Session    // etcd session
	mutex      *concurrency.Mutex      // etcd mutex
	election   *concurrency.Election   // etcd election
	hashring   *consistent.Consistent  // consistent hash range
	neighbor   map[string]zkmq.Storage // register neighbor nodes
	isMaster   bool                    // master node flag
	masterNode string                  // masterNode name
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
		replicas: config.Get("replicas").Int(2),
	}, nil
}

func (cluster *clusterImpl) Start() error {
	return cluster.bootstrap()
}

func (cluster *clusterImpl) Name() string {
	return cluster.nodeName
}

func (cluster *clusterImpl) TopicOffset(topic string) (uint64, error) {
	return 0, nil
}

func (cluster *clusterImpl) TopicStorage(topic string) ([]zkmq.Storage, error) {

	nodes, err := cluster.hashring.GetN(topic, cluster.replicas)

	if err != nil {
		return nil, xerrors.Wrapf(err, "find topic %s write storage error", topic)
	}

	cluster.RLock()
	defer cluster.RUnlock()

	var storages []zkmq.Storage

	for _, node := range nodes {
		storage, ok := cluster.neighbor[node]

		if !ok {
			return nil, xerrors.New(fmt.Sprintf("can't find storage %s", node))
		}

		storages = append(storages, storage)
	}

	return storages, nil
}
