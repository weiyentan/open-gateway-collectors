package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendBatch_Success(t *testing.T) {
	var receivedReq IngestRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header.
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(IngestResponse{
			BatchID:        "batch-123",
			AcceptedCount:  1,
			RejectedCount:  0,
			Results:        []BatchResult{{Index: 0, Status: "accepted"}},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-token", "my-host")
	resp, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
		Records: []IngestRecord{
			{SourceRecordID: "rec-1", Model: "gpt-4"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BatchID != "batch-123" {
		t.Fatalf("expected batch-123, got %s", resp.BatchID)
	}
	if resp.AcceptedCount != 1 {
		t.Fatalf("expected AcceptedCount=1, got %d", resp.AcceptedCount)
	}
	if len(resp.Results) != 1 || resp.Results[0].Status != "accepted" {
		t.Fatalf("unexpected results: %+v", resp.Results)
	}
}

func TestSendBatch_PartialSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(IngestResponse{
			BatchID:        "batch-456",
			AcceptedCount:  2,
			RejectedCount:  1,
			Results: []BatchResult{
				{Index: 0, Status: "accepted"},
				{Index: 1, Status: "accepted"},
				{Index: 2, Status: "rejected", Reason: "duplicate"},
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	resp, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BatchID != "batch-456" {
		t.Fatalf("expected batch-456, got %s", resp.BatchID)
	}
	if resp.AcceptedCount != 2 {
		t.Fatalf("expected AcceptedCount=2, got %d", resp.AcceptedCount)
	}
	if resp.RejectedCount != 1 {
		t.Fatalf("expected RejectedCount=1, got %d", resp.RejectedCount)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(resp.Results))
	}
	if resp.Results[2].Reason != "duplicate" {
		t.Fatalf("expected reason 'duplicate', got %s", resp.Results[2].Reason)
	}
}

func TestSendBatch_RetryThenSuccess(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&callCount, 1)
		if count < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(IngestResponse{
			BatchID:       "batch-retry",
			AcceptedCount: 1,
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	// Speed up the test by using tiny backoff values.
	client.baseBackoff = time.Millisecond
	client.maxBackoff = 10 * time.Millisecond
	client.maxRetries = 3

	resp, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if resp.BatchID != "batch-retry" {
		t.Fatalf("expected batch-retry, got %s", resp.BatchID)
	}
	if n := atomic.LoadInt32(&callCount); n != 3 {
		t.Fatalf("expected 3 calls (initial + 2 retries), got %d", n)
	}
}

func TestSendBatch_4xxStopsRetry(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	client.baseBackoff = time.Millisecond
	client.maxBackoff = time.Millisecond

	_, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err == nil {
		t.Fatal("expected error for 4xx response")
	}
	if n := atomic.LoadInt32(&callCount); n != 1 {
		t.Fatalf("expected exactly 1 call (no retry on 4xx), got %d", n)
	}
}

func TestSendBatch_4xxUnauthorized(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	client.baseBackoff = time.Millisecond
	client.maxBackoff = time.Millisecond

	_, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err == nil {
		t.Fatal("expected error for 401")
	}
	if n := atomic.LoadInt32(&callCount); n != 1 {
		t.Fatalf("expected 1 call (no retry on 4xx), got %d", n)
	}
}

func TestSendBatch_RetryExhausted(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	client.baseBackoff = time.Millisecond
	client.maxBackoff = 10 * time.Millisecond
	client.maxRetries = 3

	_, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	// Initial + 3 retries = 4 total attempts.
	if n := atomic.LoadInt32(&callCount); n != 4 {
		t.Fatalf("expected 4 calls (initial + 3 retries), got %d", n)
	}
}

func TestSendBatch_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")
	client.baseBackoff = time.Millisecond
	client.maxBackoff = time.Millisecond

	_, err := client.SendBatch(ctx, &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err == nil {
		t.Fatal("expected context deadline exceeded error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestSendBatch_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancelled

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "host")

	_, err := client.SendBatch(ctx, &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err == nil {
		t.Fatal("expected context cancelled error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
}

func TestSendBatch_SetsClientHostname(t *testing.T) {
	var receivedHostname string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req IngestRequest
		json.NewDecoder(r.Body).Decode(&req)
		receivedHostname = req.ClientHostname
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(IngestResponse{BatchID: "b1"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "expected-hostname")
	_, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedHostname != "expected-hostname" {
		t.Fatalf("expected hostname 'expected-hostname', got '%s'", receivedHostname)
	}
}

func TestSendBatch_ClientHostnameOverridesRequest(t *testing.T) {
	var receivedHostname string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req IngestRequest
		json.NewDecoder(r.Body).Decode(&req)
		receivedHostname = req.ClientHostname
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(IngestResponse{BatchID: "b1"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token", "constructor-hostname")
	_, err := client.SendBatch(context.Background(), &IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: "0.1.0",
		SourceDatabaseID: "db-1",
		ClientHostname:   "should-be-overridden",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedHostname != "constructor-hostname" {
		t.Fatalf("expected hostname 'constructor-hostname', got '%s'", receivedHostname)
	}
}

func TestMapToIngestRecord_WithCost(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	record := UsageRecord{
		SourceRecordID:   "rec-1",
		SessionID:        "sess-1",
		Model:            "gpt-4",
		InputTokens:      100,
		OutputTokens:     50,
		TokensCacheRead:  10,
		TokensCacheWrite: 5,
		EstimatedCostUSD: 0.0035,
		OccurredAt:       now,
	}

	result := MapToIngestRecord(record)

	if result.SourceRecordID != "rec-1" {
		t.Errorf("SourceRecordID = %q, want %q", result.SourceRecordID, "rec-1")
	}
	if result.SessionID != "sess-1" {
		t.Errorf("SessionID = %q, want %q", result.SessionID, "sess-1")
	}
	if result.Model != "gpt-4" {
		t.Errorf("Model = %q, want %q", result.Model, "gpt-4")
	}
	if result.InputTokens != 100 {
		t.Errorf("InputTokens = %d, want %d", result.InputTokens, 100)
	}
	if result.OutputTokens != 50 {
		t.Errorf("OutputTokens = %d, want %d", result.OutputTokens, 50)
	}
	if result.CachedTokens != 15 {
		t.Errorf("CachedTokens = %d, want %d", result.CachedTokens, 15)
	}
	if result.EstimatedCostUSD == nil {
		t.Fatal("ExpectedCostUSD is nil, want non-nil")
	}
	if *result.EstimatedCostUSD != "0.0035" {
		t.Errorf("EstimatedCostUSD = %q, want %q", *result.EstimatedCostUSD, "0.0035")
	}
	if result.ReportedAt != "2025-01-15T10:30:00Z" {
		t.Errorf("ReportedAt = %q, want %q", result.ReportedAt, "2025-01-15T10:30:00Z")
	}
}

func TestMapToIngestRecord_ZeroCost(t *testing.T) {
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	record := UsageRecord{
		SourceRecordID:   "rec-2",
		SessionID:        "sess-2",
		Model:            "gpt-3.5-turbo",
		InputTokens:      200,
		OutputTokens:     100,
		TokensCacheRead:  0,
		TokensCacheWrite: 0,
		EstimatedCostUSD: 0,
		OccurredAt:       now,
	}

	result := MapToIngestRecord(record)

	if result.EstimatedCostUSD != nil {
		t.Errorf("ExpectedCostUSD = %q, want nil for zero cost", *result.EstimatedCostUSD)
	}
	if result.CachedTokens != 0 {
		t.Errorf("CachedTokens = %d, want 0", result.CachedTokens)
	}
}

func TestMapToIngestRecord_OnlyCacheRead(t *testing.T) {
	record := UsageRecord{
		TokensCacheRead:  50,
		TokensCacheWrite: 0,
		EstimatedCostUSD: 0.001,
		OccurredAt:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	result := MapToIngestRecord(record)
	if result.CachedTokens != 50 {
		t.Errorf("CachedTokens = %d, want 50", result.CachedTokens)
	}
}

func TestMapToIngestRecord_OnlyCacheWrite(t *testing.T) {
	record := UsageRecord{
		TokensCacheRead:  0,
		TokensCacheWrite: 25,
		EstimatedCostUSD: 0.001,
		OccurredAt:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	result := MapToIngestRecord(record)
	if result.CachedTokens != 25 {
		t.Errorf("CachedTokens = %d, want 25", result.CachedTokens)
	}
}

func TestMapToIngestRecord_LargeCost(t *testing.T) {
	record := UsageRecord{
		EstimatedCostUSD: 1234.56789,
		OccurredAt:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	result := MapToIngestRecord(record)
	if result.EstimatedCostUSD == nil {
		t.Fatal("ExpectedCostUSD is nil, want non-nil")
	}
	if *result.EstimatedCostUSD != "1234.56789" {
		t.Errorf("EstimatedCostUSD = %q, want %q", *result.EstimatedCostUSD, "1234.56789")
	}
}

func TestNewClient_SetsDefaults(t *testing.T) {
	client := NewClient("http://example.com", "tok", "h")
	if client.baseURL != "http://example.com" {
		t.Errorf("baseURL = %q, want %q", client.baseURL, "http://example.com")
	}
	if client.token != "tok" {
		t.Errorf("token = %q, want %q", client.token, "tok")
	}
	if client.hostname != "h" {
		t.Errorf("hostname = %q, want %q", client.hostname, "h")
	}
	if client.maxRetries != defaultMaxRetries {
		t.Errorf("maxRetries = %d, want %d", client.maxRetries, defaultMaxRetries)
	}
	if client.maxBackoff != defaultMaxBackoff {
		t.Errorf("maxBackoff = %v, want %v", client.maxBackoff, defaultMaxBackoff)
	}
	if client.baseBackoff != defaultBaseBackoff {
		t.Errorf("baseBackoff = %v, want %v", client.baseBackoff, defaultBaseBackoff)
	}
	if client.httpClient.Timeout != defaultHTTPTimeout {
		t.Errorf("httpClient.Timeout = %v, want %v", client.httpClient.Timeout, defaultHTTPTimeout)
	}
}

func TestNewClient_TrailingSlash(t *testing.T) {
	client := NewClient("http://example.com/", "tok", "h")
	if client.baseURL != "http://example.com" {
		t.Errorf("baseURL with trailing slash = %q, want %q", client.baseURL, "http://example.com")
	}
}
