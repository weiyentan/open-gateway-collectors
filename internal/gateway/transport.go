package gateway

import "context"

// Transport sends an IngestRequest to the Gateway (via HTTP) or to an
// intermediary (e.g. Kafka) and returns the response.
type Transport interface {
	SendBatch(ctx context.Context, req *IngestRequest) (*IngestResponse, error)
}
