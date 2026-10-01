package app

import (
	"errors"

	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/serviceauth"
	kgo "github.com/segmentio/kafka-go"
)

func (p ServeParams) kafkaSecurity(registry *serviceauth.Registry) ([]serviceauth.KafkaCredential, *kgo.Dialer, error) {
	credentials := registry.KafkaCredentials()
	var reader *serviceauth.KafkaCredential
	var read, write bool
	for i := range credentials {
		item := &credentials[i]
		if item.Accounting {
			if reader != nil {
				return nil, nil, errors.New("exactly one Kafka accounting credential is allowed")
			}
			reader = item
		}
		if p.EnableKafka && item.Read && item.Write {
			return nil, nil, errors.New("embedded Kafka read and write users must be distinct")
		}
		read = read || item.Read
		write = write || item.Write
	}
	if p.EnableKafka && (!read || !write) {
		return nil, nil, errors.New("embedded Kafka requires distinct read and write credentials")
	}
	if p.EnableKafka && p.EnableAccounting && reader == nil {
		return nil, nil, errors.New("embedded Kafka accounting requires one accounting reader")
	}
	if !p.EnableAccounting {
		reader = nil
	}
	if !p.EnableKafka {
		credentials = nil
	}
	dialer, err := kafka.NewDialer(reader)
	return credentials, dialer, err
}
