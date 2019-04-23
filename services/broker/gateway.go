package broker

import (
	"context"
	"sync"

	"github.com/dynamicgo/xerrors"

	"github.com/zkmq/zkmq"
)

func (broker *brokerImpl) Push(ctx context.Context, record *zkmq.Record) (*zkmq.PushResponse, error) {
	// broker.Cluster.TopicStorage(zkmq.Record)

	offset, err := broker.Cluster.TopicOffset(record.Topic)

	if err != nil {
		return nil, xerrors.Wrapf(err, "get topic %s offset error", record.Topic)
	}

	storages, err := broker.Cluster.TopicStorage(record.Topic)

	if err != nil {
		return nil, err
	}

	record.Offset = offset

	err = callN(ctx, storages, func(storage zkmq.Storage) error {
		return storage.Write(record)
	})

	if err != nil {
		return nil, err
	}

	return &zkmq.PushResponse{
		Offset: offset,
	}, nil
}

func callN(ctx context.Context, storages []zkmq.Storage, f func(zkmq.Storage) error) error {

	if len(storages) == 0 {
		return nil
	}

	var wg sync.WaitGroup

	wg.Add(len(storages))

	errors := make([]error, len(storages))

	for k, v := range storages {

		i := k
		storage := v

		go func() {
			err := f(storage)

			if err != nil {
				errors[i] = err
			}

			wg.Done()
		}()
	}

	c := make(chan interface{})
	defer close(c)

	go func() {
		wg.Wait()

		c <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c:
		for _, e := range errors {
			if e != nil {
				return e
			}
		}

		return nil
	}
}
