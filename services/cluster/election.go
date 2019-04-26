package cluster

import (
	"context"
	"fmt"

	"github.com/dynamicgo/xerrors"
	"github.com/zkmq/zkmq"
)

func (cluster *clusterImpl) doElection() {
	for {
		if err := cluster.election.Campaign(context.Background(), cluster.nodeName); err != nil {
			cluster.ErrorF("election return err: %s", err)

			continue
		}

		break
	}
}

func (cluster *clusterImpl) handleElectionEvt() {
	evt := cluster.election.Observe(context.Background())
	for resp := range evt {
		if len(resp.Kvs) == 0 {
			continue
		}

		masterNode := string(resp.Kvs[len(resp.Kvs)-1].Value)

		cluster.updateMaster(masterNode)

	}
}

func (cluster *clusterImpl) updateMaster(masterNode string) {

	cluster.Lock()
	defer cluster.Unlock()

	cluster.InfoF("elected master node: %s", masterNode)

	if cluster.isMaster {
		if cluster.nodeName != masterNode {
			go cluster.doElection()
		} else {
			cluster.isMaster = false
		}
	}

	cluster.masterNode = masterNode
}

func (cluster *clusterImpl) tryGetMasterBroker() (*remoteBroker, error) {

	cluster.RLock()
	defer cluster.RUnlock()

	storage, ok := cluster.neighbor[cluster.masterNode]

	if !ok {
		return nil, xerrors.Wrapf(zkmq.ErrMaster, "can't find master")
	}

	remote, ok := storage.(*remoteBroker)

	if !ok {
		return nil, xerrors.New(fmt.Sprintf("inner error,cast remote master node %s", cluster.masterNode))
	}

	return remote, nil
}
