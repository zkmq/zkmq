package cluster

import (
	"context"

	"github.com/zkmq/zkmq"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"google.golang.org/grpc"
)

func (cluster *clusterImpl) listenNodeEvt() {

	cluster.InfoF("start node evt watch ...")

	rch := cluster.Etcd.Watch(context.Background(), "node/", clientv3.WithPrefix())

	for wresp := range rch {
		for _, ev := range wresp.Events {

			cluster.DebugF("%s neighbor node %s %s", ev.Type, ev.Kv.Key, ev.Kv.Value)

			cluster.handleNodeEvent(ev)
		}
	}
}

func (cluster *clusterImpl) handleNodeEvent(ev *clientv3.Event) {
	cluster.Lock()
	switch ev.Type {
	case mvccpb.PUT:

		conn, err := grpc.Dial(string(ev.Kv.Value), grpc.WithInsecure())

		if err != nil {
			cluster.ErrorF("dial to broker %s error: %s", string(ev.Kv.Value), err)
		} else {
			cluster.neighbor[string(ev.Kv.Key)] = zkmq.NewBrokerClient(conn)
		}

	case mvccpb.DELETE:
		delete(cluster.neighbor, string(ev.Kv.Key))
	}
	cluster.Unlock()

	switch ev.Type {
	case mvccpb.PUT:
		cluster.hashring.Add(string(ev.Kv.Key))
	case mvccpb.DELETE:
		cluster.hashring.Remove(string(ev.Kv.Key))
	}
}
