package kafka

import (
	"context"
	"errors"
	"net"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// Credential identifies one Kafka client. The embedded broker grants Read and
// Write access independently; the accounting listener uses Read.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type BrokerAccess struct {
	Read  Credential `json:"read"`
	Write Credential `json:"write"`
}

// NewDialer uses SCRAM-SHA-512 for the accounting reader. The embedded broker
// also accepts PLAIN for independently configured writer clients.
func NewDialer(credential *Credential) (*kgo.Dialer, error) {
	if credential == nil {
		return nil, nil
	}
	if credential.Username == "" || credential.Password == "" {
		return nil, errors.New("Kafka read username and password are required")
	}
	mechanism, err := scram.Mechanism(scram.SHA512, credential.Username, credential.Password)
	if err != nil {
		return nil, errors.New("invalid Kafka read credentials")
	}
	auth := kgo.Dialer{SASLMechanism: mechanism}
	return &kgo.Dialer{Timeout: 10 * time.Second, DialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return conn, err
		}
		// Bound SASL as well as TCP, then detach cancellation before reuse.
		stop := context.AfterFunc(ctx, func() { conn.Close() })
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
