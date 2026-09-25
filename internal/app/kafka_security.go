package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/livepeer/clearinghouse/internal/kafka"
	kgo "github.com/segmentio/kafka-go"
)

const maxKafkaAuthFileBytes = 64 << 10

func (p ServeParams) kafkaSecurity() (*kafka.BrokerAccess, *kgo.Dialer, error) {
	var access *kafka.BrokerAccess
	if p.KafkaAuthFile != "" {
		file, err := os.Open(p.KafkaAuthFile)
		if err != nil {
			return nil, nil, fmt.Errorf("open Kafka auth file: %w", err)
		}
		defer file.Close()
		var parsed kafka.BrokerAccess
		destination := any(&parsed)
		if !p.EnableKafka {
			destination = &struct{ Read *kafka.Credential }{&parsed.Read}
		}
		limited := &io.LimitedReader{R: file, N: maxKafkaAuthFileBytes + 1}
		decoder := json.NewDecoder(limited)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(destination); err != nil {
			return nil, nil, fmt.Errorf("decode Kafka auth file: %w", err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) || limited.N == 0 {
			return nil, nil, errors.New("Kafka auth file must contain one JSON object within 64 KiB")
		}
		credentials := []kafka.Credential{parsed.Read}
		if p.EnableKafka {
			credentials = append(credentials, parsed.Write)
		}
		for i, credential := range credentials {
			role := []string{"read", "write"}[i]
			if credential.Username == "" || credential.Password == "" {
				return nil, nil, fmt.Errorf("Kafka %s username and password are required", role)
			}
			if credential.Username == "*" || strings.ContainsRune(credential.Username, 0) || strings.ContainsRune(credential.Password, 0) {
				return nil, nil, fmt.Errorf("invalid Kafka %s credential", role)
			}
		}
		if p.EnableKafka && parsed.Read.Username == parsed.Write.Username {
			return nil, nil, errors.New("Kafka read and write users must be distinct")
		}
		access = &parsed
	}
	var reader *kafka.Credential
	if access != nil && p.EnableAccounting {
		reader = &access.Read
	}
	dialer, err := kafka.NewDialer(reader)
	return access, dialer, err
}
