package cluster

import (
	"context"
	"time"

	"github.com/dynamicgo/xerrors"
	"github.com/zkmq/zkmq"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"google.golang.org/grpc"
)

func (cluster *clusterImpl) bootstrap() error {

	if err := cluster.newEtcdSession(); err != nil {
		return err
	}

	if err := cluster.registerService(); err != nil {
		return err
	}

	if err := cluster.updateNode(); err != nil {
		return err
	}

	if err := cluster.fetchClusterNodes(); err != nil {
		return err
	}

	go cluster.listenNodeEvt()
	go cluster.handleElectionEvt()

	go cluster.doElection()

	return nil
}

func (cluster *clusterImpl) newEtcdSession() error {
	session, err := concurrency.NewSession(cluster.Etcd, concurrency.WithTTL(int(cluster.ttl/time.Second)))

	if err != nil {
		return xerrors.Wrapf(err, "create etcd session error")
	}

	cluster.session = session

	cluster.mutex = concurrency.NewMutex(cluster.session, "/boostrap/mutex")

	cluster.election = concurrency.NewElection(cluster.session, "/master/election")

	return nil
}

func (cluster *clusterImpl) registerService() error {

	if err := cluster.lock(); err != nil {
		return err
	}

	defer cluster.unlock()

	// register service node

	getResp, err := cluster.Etcd.Get(context.Background(), "service/"+cluster.nodeName)

	if err != nil {
		return xerrors.Wrapf(err, "acquire bootstrap lock error")
	}

	if getResp.Count > 0 {
		return xerrors.Wrapf(zkmq.ErrExists, "node %s exists", cluster.nodeName)
	}

	_, err = cluster.Etcd.Put(context.Background(), "service/"+cluster.nodeName, cluster.laddr, clientv3.WithLease(cluster.session.Lease()))

	if err != nil {
		return xerrors.Wrapf(err, "register node %s error", cluster.nodeName)
	}

	return nil
}

func (cluster *clusterImpl) updateNode() error {
	getResp, err := cluster.Etcd.Get(context.Background(), "node/"+cluster.nodeName)

	if err != nil {
		return xerrors.Wrapf(err, "get zkmq nodes error")
	}

	if getResp.Count != 0 {
		if string(getResp.Kvs[0].Value) == cluster.laddr {
			cluster.DebugF("ignore update node(%s,%s) info", cluster.nodeName, cluster.laddr)
			return nil
		}
	}

	_, err = cluster.Etcd.Put(context.Background(), "node/"+cluster.nodeName, cluster.laddr)

	if err != nil {
		return xerrors.Wrapf(err, "register consistent node %s error", cluster.nodeName)
	}

	return nil
}

func (cluster *clusterImpl) fetchClusterNodes() error {
	getResp, err := cluster.Etcd.Get(context.Background(), "node/", clientv3.WithPrefix())

	if err != nil {
		return xerrors.Wrapf(err, "get zkmq nodes error")
	}

	nodes := make(map[string]zkmq.BrokerClient)

	for _, kv := range getResp.Kvs {

		cluster.DebugF("consistent hash node(%s,%s)", string(kv.Key), string(kv.Value))

		if string(kv.Key) != "node/"+cluster.nodeName {
			conn, err := grpc.Dial(string(kv.Value), grpc.WithInsecure())

			if err != nil {
				return xerrors.Wrapf(err, "dial to broker %s error", string(kv.Value))
			}

			nodes[string(kv.Key)] = zkmq.NewBrokerClient(conn)
		}

		cluster.hashring.Add(string(kv.Key))
	}

	cluster.neighbor = nodes

	return nil
}
