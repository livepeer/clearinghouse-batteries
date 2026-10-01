package kafka

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/livepeer/clearinghouse/internal/serviceauth"
	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func brokerConfig(topic string, credentials []serviceauth.KafkaCredential) (*minikafka.SASLConfig, *minikafka.AuthorizationConfig) {
	users := make(map[string]string, len(credentials))
	grants := make([]minikafka.TopicGrant, 0, len(credentials))
	for _, user := range credentials {
		users[user.Username] = user.Password
		if user.Read {
			grants = append(grants, minikafka.TopicGrant{User: user.Username, Topic: topic, Action: minikafka.TopicRead})
		}
		if user.Write {
			grants = append(grants, minikafka.TopicGrant{User: user.Username, Topic: topic, Action: minikafka.TopicWrite})
		}
	}
	return &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLSCRAMSHA512}, Users: users}, &minikafka.AuthorizationConfig{Grants: grants}
}

// NewDialer uses SCRAM-SHA-512 for accounting.
func NewDialer(credential *serviceauth.KafkaCredential) (*kgo.Dialer, error) {
	if credential == nil {
		return nil, nil
	}
	if credential.Username == "" || credential.Password == "" {
		return nil, errors.New("Kafka accounting username and password are required")
	}
	mechanism, err := scram.Mechanism(scram.SHA512, credential.Username, credential.Password)
	if err != nil {
		return nil, errors.New("invalid Kafka accounting credentials")
	}
	auth := kgo.Dialer{SASLMechanism: mechanism}
	return &kgo.Dialer{Timeout: 10 * time.Second, DialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		d := auth
		d.DialFunc = func(context.Context, string, string) (net.Conn, error) { return conn, nil }
		_, err = d.DialContext(ctx, network, address)
		if !stop() {
			err = ctx.Err()
		}
		if err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}}, nil
}
