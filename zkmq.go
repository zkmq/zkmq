package zkmq

//go:generate protoc  --go_out=plugins=grpc:. zkmq.proto

// Storage .
type Storage interface {
	Write(record *Record) (uint64, error)
	Offset(topic string) uint64
	ConsumerOffset(topic string, consumer string) uint64
	CommitOffset(topic, consumer string, offset uint64) (uint64, error)
	Read(topic string, consumer string, number uint64) ([]*Record, error)
}

// Metadata .
type Metadata interface {
	TopicHeader(topic string) (uint64, error)
	CommitTopicHeader(topic string, offset uint64) (uint64, error)
	ConsumerOffset(topic string, consumer string) (uint64, error)
	CommitOffset(topic, consumer string, offset uint64) (uint64, error)
}
