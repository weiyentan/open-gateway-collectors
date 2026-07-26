package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaClient implements Transport by producing serialised IngestRequest
// messages to a Kafka topic. Each message is keyed by source_database_id
// for partition co-location.
type KafkaClient struct {
	client *kgo.Client
	topic  string
}

// NewKafkaClient creates a KafkaClient connected to the given brokers.
// topic is the Kafka topic to produce to. clientID is the Kafka client
// identifier sent to brokers (if empty, the franz-go default "kgo" is used).
func NewKafkaClient(brokers []string, topic, clientID string) (*KafkaClient, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka new client: %w", err)
	}
	return &KafkaClient{
		client: cl,
		topic:  topic,
	}, nil
}

// SendBatch serialises req to JSON and produces it to the configured Kafka
// topic. The message is keyed by req.SourceDatabaseID (or a placeholder if
// empty) for deterministic partitioning. The call blocks until Kafka
// acknowledges the produce (or the context is cancelled).
//
// On success it returns an IngestResponse with AcceptedCount set to the
// number of records in the batch. On failure it returns an error; no
// response is returned.
func (k *KafkaClient) SendBatch(ctx context.Context, req *IngestRequest) (*IngestResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Key by source_database_id so all records from the same database
	// land in the same partition.
	key := []byte(req.SourceDatabaseID)
	if len(key) == 0 {
		key = []byte("unknown")
	}

	record := &kgo.Record{
		Topic: k.topic,
		Key:   key,
		Value: body,
	}

	// Block until ack from all required in-sync replicas.
	result := k.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return nil, fmt.Errorf("kafka produce: %w", err)
	}

	return &IngestResponse{
		AcceptedCount: len(req.Records),
	}, nil
}
