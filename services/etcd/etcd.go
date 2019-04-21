package etcd

import (
	"strings"
	"time"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/xerrors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
)

// New .
func New(config config.Config) (*clientv3.Client, error) {

	remote := config.Get("remote").StringSlice(nil)

	if remote == nil {
		return nil, xerrors.New("expect etcd remote address")
	}

	dialTimeout := config.Get("dialTimeout").Duration(time.Second * 3)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   remote,
		DialTimeout: dialTimeout,
	})

	if err != nil {
		return nil, xerrors.Wrapf(err, "create etcd clientv3 error")
	}

	ns := config.Get("namespace").String("zkmq/")

	if !strings.HasSuffix(ns, "/") {
		ns = ns + "/"
	}

	client.KV = namespace.NewKV(client.KV, ns)
	client.Watcher = namespace.NewWatcher(client.Watcher, ns)
	client.Lease = namespace.NewLease(client.Lease, ns)

	return client, nil
}
