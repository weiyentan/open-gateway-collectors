// Package heartbeat provides the heartbeat builder for the collector.
// A heartbeat is an empty ingest batch sent to the Gateway to signal the
// collector is alive and the source database is still active.
package heartbeat

import "github.com/opencode-gateway/collectors/internal/gateway"

// BuildHeartbeat creates an IngestRequest with an empty records slice.
// The Gateway treats empty batches as heartbeats — no usage rows are
// inserted, only the source database's last_seen_at timestamp is updated.
func BuildHeartbeat(sourceDatabaseID, collectorVersion, clientHostname string) *gateway.IngestRequest {
	return &gateway.IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: collectorVersion,
		ClientHostname:   clientHostname,
		SourceDatabaseID: sourceDatabaseID,
		Records:          make([]gateway.IngestRecord, 0),
	}
}
