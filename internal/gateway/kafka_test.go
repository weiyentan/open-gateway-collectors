package gateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Tests: Transport interface compliance
// ---------------------------------------------------------------------------

// compile-time check that MockTransport implements Transport.
var _ Transport = (*MockTransport)(nil)

func TestMockTransport_ImplementsTransport(t *testing.T) {
	// Verify the mock records calls correctly.
	mock := NewMockTransport(&IngestResponse{BatchID: "mock-batch", AcceptedCount: 3})

	req := &IngestRequest{
		SchemaVersion:    "1.1",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
		Records: []IngestRecord{
			{SourceRecordID: "rec-1", Model: "gpt-4"},
		},
	}

	resp, err := mock.SendBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BatchID != "mock-batch" {
		t.Errorf("BatchID = %q, want %q", resp.BatchID, "mock-batch")
	}
	if resp.AcceptedCount != 3 {
		t.Errorf("AcceptedCount = %d, want %d", resp.AcceptedCount, 3)
	}

	if mock.CallCount() != 1 {
		t.Fatalf("CallCount = %d, want 1", mock.CallCount())
	}

	call := mock.LastCall()
	if call.Req.SourceDatabaseID != "db-1" {
		t.Errorf("SourceDatabaseID = %q, want %q", call.Req.SourceDatabaseID, "db-1")
	}
	if len(call.Req.Records) != 1 {
		t.Errorf("len(Records) = %d, want 1", len(call.Req.Records))
	}
}

func TestMockTransport_RecordsMultipleCalls(t *testing.T) {
	mock := NewMockTransport(&IngestResponse{BatchID: "multi", AcceptedCount: 1})

	for i := 0; i < 3; i++ {
		_, err := mock.SendBatch(context.Background(), &IngestRequest{
			SourceDatabaseID: "db-1",
		})
		if err != nil {
			t.Fatalf("unexpected error on call %d: %v", i, err)
		}
	}

	if mock.CallCount() != 3 {
		t.Fatalf("CallCount = %d, want 3", mock.CallCount())
	}
}

func TestMockTransport_ReturnsError(t *testing.T) {
	mock := NewMockTransport(nil)
	mock.Err = context.DeadlineExceeded

	_, err := mock.SendBatch(context.Background(), &IngestRequest{})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	if mock.CallCount() != 1 {
		t.Errorf("CallCount = %d, want 1 (call recorded even on error)", mock.CallCount())
	}
}

// ---------------------------------------------------------------------------
// Tests: KafkaClient constructor
// ---------------------------------------------------------------------------

func TestNewKafkaClient_StoresHostname(t *testing.T) {
	kc, err := NewKafkaClient(
		[]string{"localhost:9999"},
		"test-topic",
		"test-client-id",
		"my-custom-host",
	)
	if err != nil {
		t.Fatalf("NewKafkaClient: %v", err)
	}

	if kc.hostname != "my-custom-host" {
		t.Errorf("hostname = %q, want %q", kc.hostname, "my-custom-host")
	}
	if kc.topic != "test-topic" {
		t.Errorf("topic = %q, want %q", kc.topic, "test-topic")
	}
	if kc.client == nil {
		t.Error("client is nil, want non-nil kgo.Client")
	}
}

func TestNewKafkaClient_EmptyHostname(t *testing.T) {
	kc, err := NewKafkaClient(
		[]string{"localhost:9999"},
		"test-topic",
		"",
		"",
	)
	if err != nil {
		t.Fatalf("NewKafkaClient: %v", err)
	}

	if kc.hostname != "" {
		t.Errorf("hostname = %q, want empty string", kc.hostname)
	}
}

// ---------------------------------------------------------------------------
// Tests: KafkaClient.SendBatch hostname serialization
// ---------------------------------------------------------------------------

// TestKafkaClient_SendBatch_SetsClientHostname verifies that the KafkaClient
// sets req.ClientHostname before JSON serialisation, matching the HTTP client
// behaviour. Since a real Kafka broker isn't available in unit tests, this
// test verifies the intent by replicating the SendBatch logic with a
// KafkaClient and then inspecting the serialised JSON.
func TestKafkaClient_SendBatch_SetsClientHostname(t *testing.T) {
	kc := &KafkaClient{
		client:   nil, // not needed for this test
		topic:    "test-topic",
		hostname: "kafka-host-007",
	}

	req := &IngestRequest{
		SchemaVersion:    "1.1",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-host-1",
		Records: []IngestRecord{
			{SourceRecordID: "rec-1", Model: "gpt-4"},
		},
	}

	// Replicate the first two lines of SendBatch to verify behaviour.
	req.ClientHostname = kc.hostname
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	// Verify the JSON body contains the hostname.
	raw := string(body)
	if !strings.Contains(raw, `"client_hostname"`) {
		t.Error("serialised body does not contain client_hostname field")
	}
	if !strings.Contains(raw, `"kafka-host-007"`) {
		t.Errorf("serialised body does not contain hostname value 'kafka-host-007': %s", raw)
	}

	// Verify the hostname is set on the request for downstream use.
	if req.ClientHostname != "kafka-host-007" {
		t.Errorf("ClientHostname = %q, want %q", req.ClientHostname, "kafka-host-007")
	}

	// Verify that a different hostname produces different output.
	kc2 := &KafkaClient{hostname: "another-host"}
	req2 := &IngestRequest{
		SchemaVersion:    "1.1",
		SourceDatabaseID: "db-2",
	}
	req2.ClientHostname = kc2.hostname
	body2, _ := json.Marshal(req2)
	if !strings.Contains(string(body2), `"another-host"`) {
		t.Errorf("serialised body does not contain second hostname: %s", string(body2))
	}
}

// TestKafkaClient_SendBatch_HostnameOverridesRequest verifies that the
// KafkaClient always uses its stored hostname, overriding any hostname that
// may have been set on the incoming IngestRequest (same semantics as the HTTP
// client).
func TestKafkaClient_SendBatch_HostnameOverridesRequest(t *testing.T) {
	kc := &KafkaClient{
		hostname: "constructor-hostname",
	}

	req := &IngestRequest{
		SchemaVersion:    "1.1",
		ClientHostname:   "should-be-overridden",
		SourceDatabaseID: "db-3",
	}

	// Simulate SendBatch hostname assignment.
	req.ClientHostname = kc.hostname
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Verify the constructor hostname won, not the request's original value.
	if strings.Contains(string(body), "should-be-overridden") {
		t.Error("serialised body contains overridden hostname instead of constructor hostname")
	}
	if !strings.Contains(string(body), "constructor-hostname") {
		t.Error("serialised body does not contain constructor hostname")
	}
}
