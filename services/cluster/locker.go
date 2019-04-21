package cluster

import (
	"context"

	"github.com/dynamicgo/xerrors"
)

func (cluster *clusterImpl) lock() error {

	if err := cluster.mutex.Lock(context.Background()); err != nil {
		return xerrors.Wrapf(err, "acquire bootstrap lock error")
	}

	return nil
}

func (cluster *clusterImpl) unlock() error {
	if err := cluster.mutex.Unlock(context.Background()); err != nil {
		return xerrors.Wrapf(err, "release bootstrap lock error")
	}

	return nil
}
