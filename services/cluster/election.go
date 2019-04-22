package cluster

import "context"

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
}
