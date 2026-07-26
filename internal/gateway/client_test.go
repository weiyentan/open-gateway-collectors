package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencode-gateway/collectors/internal/sqlite"
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
		ProviderID:       "openai",
		Mode:             "chat",
		Agent:            "code-editor",
		ProjectID:        "proj-1",
		WorkspaceID:      "ws-1",
		ParentSessionID:  "parent-sess-1",
		ReasoningTokens:  30,
		FinishReason:     "stop",
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
	if result.Provider != "openai" {
		t.Errorf("Provider = %q, want %q", result.Provider, "openai")
	}
	if result.Mode != "chat" {
		t.Errorf("Mode = %q, want %q", result.Mode, "chat")
	}
	// Enrichment fields.
	if result.Agent != "code-editor" {
		t.Errorf("Agent = %q, want %q", result.Agent, "code-editor")
	}
	if result.ProjectID != "proj-1" {
		t.Errorf("ProjectID = %q, want %q", result.ProjectID, "proj-1")
	}
	if result.WorkspaceID != "ws-1" {
		t.Errorf("WorkspaceID = %q, want %q", result.WorkspaceID, "ws-1")
	}
	if result.ParentSessionID != "parent-sess-1" {
		t.Errorf("ParentSessionID = %q, want %q", result.ParentSessionID, "parent-sess-1")
	}
	if result.ReasoningTokens != 30 {
		t.Errorf("ReasoningTokens = %d, want %d", result.ReasoningTokens, 30)
	}
	if result.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want %q", result.FinishReason, "stop")
	}
	// Cache split fields.
	if result.CacheReadTokens != 10 {
		t.Errorf("CacheReadTokens = %d, want %d", result.CacheReadTokens, 10)
	}
	if result.CacheWriteTokens != 5 {
		t.Errorf("CacheWriteTokens = %d, want %d", result.CacheWriteTokens, 5)
	}
	if result.InputTokens != 100 {
		t.Errorf("InputTokens = %d, want %d", result.InputTokens, 100)
	}
	if result.OutputTokens != 50 {
		t.Errorf("OutputTokens = %d, want %d", result.OutputTokens, 50)
	}
	// Backward-compatible CachedTokens sum.
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
		Agent:            "cli-agent",
		ProjectID:        "proj-2",
		WorkspaceID:      "ws-2",
		ParentSessionID:  "parent-2",
		ReasoningTokens:  0,
		FinishReason:     "length",
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
	// Cache split fields: zero both ways.
	if result.CacheReadTokens != 0 {
		t.Errorf("CacheReadTokens = %d, want 0", result.CacheReadTokens)
	}
	if result.CacheWriteTokens != 0 {
		t.Errorf("CacheWriteTokens = %d, want 0", result.CacheWriteTokens)
	}
	if result.CachedTokens != 0 {
		t.Errorf("CachedTokens = %d, want 0", result.CachedTokens)
	}
	// Enrichment fields.
	if result.Agent != "cli-agent" {
		t.Errorf("Agent = %q, want %q", result.Agent, "cli-agent")
	}
	if result.ProjectID != "proj-2" {
		t.Errorf("ProjectID = %q, want %q", result.ProjectID, "proj-2")
	}
	if result.WorkspaceID != "ws-2" {
		t.Errorf("WorkspaceID = %q, want %q", result.WorkspaceID, "ws-2")
	}
	if result.ParentSessionID != "parent-2" {
		t.Errorf("ParentSessionID = %q, want %q", result.ParentSessionID, "parent-2")
	}
	if result.ReasoningTokens != 0 {
		t.Errorf("ReasoningTokens = %d, want 0", result.ReasoningTokens)
	}
	if result.FinishReason != "length" {
		t.Errorf("FinishReason = %q, want %q", result.FinishReason, "length")
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
	if result.CacheReadTokens != 50 {
		t.Errorf("CacheReadTokens = %d, want 50", result.CacheReadTokens)
	}
	if result.CacheWriteTokens != 0 {
		t.Errorf("CacheWriteTokens = %d, want 0", result.CacheWriteTokens)
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
	if result.CacheReadTokens != 0 {
		t.Errorf("CacheReadTokens = %d, want 0", result.CacheReadTokens)
	}
	if result.CacheWriteTokens != 25 {
		t.Errorf("CacheWriteTokens = %d, want 25", result.CacheWriteTokens)
	}
}

func TestMapToIngestRecord_LargeCost(t *testing.T) {
	record := UsageRecord{
		TokensCacheRead:  7,
		TokensCacheWrite: 3,
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
	if result.CacheReadTokens != 7 {
		t.Errorf("CacheReadTokens = %d, want 7", result.CacheReadTokens)
	}
	if result.CacheWriteTokens != 3 {
		t.Errorf("CacheWriteTokens = %d, want 3", result.CacheWriteTokens)
	}
	if result.CachedTokens != 10 {
		t.Errorf("CachedTokens = %d, want 10", result.CachedTokens)
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

// ---------------------------------------------------------------------------
// Projection mapping tests
// ---------------------------------------------------------------------------

func TestMapToSessionContext_AllFields(t *testing.T) {
	data := sqlite.SessionContextData{
		ExternalSessionID: "sess-1",
		Title:             "My Session",
		Agent:             "claude",
		ProjectID:         "proj-1",
		ParentSessionID:   "parent-1",
		WorkspaceID:       "ws-1",
		Model:             "gpt-4",
	}
	result := MapToSessionContext(data)

	if result.ExternalSessionID != "sess-1" {
		t.Errorf("ExternalSessionID = %q, want %q", result.ExternalSessionID, "sess-1")
	}
	if result.Title != "My Session" {
		t.Errorf("Title = %q, want %q", result.Title, "My Session")
	}
	if result.Agent != "claude" {
		t.Errorf("Agent = %q, want %q", result.Agent, "claude")
	}
	if result.ProjectID != "proj-1" {
		t.Errorf("ProjectID = %q, want %q", result.ProjectID, "proj-1")
	}
	if result.ParentSessionID != "parent-1" {
		t.Errorf("ParentSessionID = %q, want %q", result.ParentSessionID, "parent-1")
	}
	if result.WorkspaceID != "ws-1" {
		t.Errorf("WorkspaceID = %q, want %q", result.WorkspaceID, "ws-1")
	}
	if result.Model != "gpt-4" {
		t.Errorf("Model = %q, want %q", result.Model, "gpt-4")
	}
}

func TestMapToSessionContext_EmptyFields(t *testing.T) {
	data := sqlite.SessionContextData{
		ExternalSessionID: "sess-min",
	}
	result := MapToSessionContext(data)

	if result.ExternalSessionID != "sess-min" {
		t.Errorf("ExternalSessionID = %q, want %q", result.ExternalSessionID, "sess-min")
	}
	if result.Title != "" {
		t.Errorf("Title should be empty, got %q", result.Title)
	}
	if result.Agent != "" {
		t.Errorf("Agent should be empty, got %q", result.Agent)
	}
}

func TestMapToProjectSnapshot_AllFields(t *testing.T) {
	data := sqlite.ProjectData{
		ExternalProjectID: "proj-1",
		Title:             "My Project",
		Worktree:          "/path/to/repo",
	}
	result := MapToProjectSnapshot(data)

	if result.ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", result.ExternalProjectID, "proj-1")
	}
	if result.Title != "My Project" {
		t.Errorf("Title = %q, want %q", result.Title, "My Project")
	}
	if result.Worktree != "/path/to/repo" {
		t.Errorf("Worktree = %q, want %q", result.Worktree, "/path/to/repo")
	}
}

func TestMapToProjectSnapshot_EmptyFields(t *testing.T) {
	data := sqlite.ProjectData{
		ExternalProjectID: "proj-min",
	}
	result := MapToProjectSnapshot(data)

	if result.ExternalProjectID != "proj-min" {
		t.Errorf("ExternalProjectID = %q, want %q", result.ExternalProjectID, "proj-min")
	}
	if result.Title != "" {
		t.Errorf("Title should be empty, got %q", result.Title)
	}
	if result.Worktree != "" {
		t.Errorf("Worktree should be empty, got %q", result.Worktree)
	}
}

func TestMapToProjectDirectorySnapshot(t *testing.T) {
	data := sqlite.ProjectDirectoryData{
		ExternalProjectID: "proj-1",
		Path:              "/src/app",
	}
	result := MapToProjectDirectorySnapshot(data)

	if result.ExternalProjectID != "proj-1" {
		t.Errorf("ExternalProjectID = %q, want %q", result.ExternalProjectID, "proj-1")
	}
	if result.Path != "/src/app" {
		t.Errorf("Path = %q, want %q", result.Path, "/src/app")
	}
}

func TestMapToTodoSnapshot(t *testing.T) {
	data := sqlite.TodoData{
		ExternalSessionID: "sess-1",
		Description:       "Fix login bug",
		Status:            "completed",
	}
	result := MapToTodoSnapshot(data)

	if result.ExternalSessionID != "sess-1" {
		t.Errorf("ExternalSessionID = %q, want %q", result.ExternalSessionID, "sess-1")
	}
	if result.Description != "Fix login bug" {
		t.Errorf("Description = %q, want %q", result.Description, "Fix login bug")
	}
	if result.Status != "completed" {
		t.Errorf("Status = %q, want %q", result.Status, "completed")
	}
}

func TestMapToTodoSnapshot_EmptyStatus(t *testing.T) {
	data := sqlite.TodoData{
		ExternalSessionID: "sess-1",
		Description:       "Review PR",
	}
	result := MapToTodoSnapshot(data)

	if result.Status != "" {
		t.Errorf("Status should be empty, got %q", result.Status)
	}
}

func TestIngestRequest_JSONSerialization_WithProjections(t *testing.T) {
	cost := "0.0035"
	req := IngestRequest{
		SchemaVersion:    "1.1",
		CollectorVersion: "0.1.0",
		ClientHostname:   "test-host",
		SourceDatabaseID: "db-1",
		Records: []IngestRecord{
			{
				SourceRecordID:   "rec-1",
				SessionID:        "sess-1",
				Model:            "gpt-4",
				Provider:         "openai",
				ReportedAt:       "2025-01-01T00:00:00Z",
				EstimatedCostUSD: &cost,
			},
		},
		SessionContexts: []SessionContext{
			{
				ExternalSessionID: "sess-1",
				Title:             "Test Session",
				Agent:             "claude",
				ProjectID:         "proj-1",
			},
		},
		ProjectSnapshots: []ProjectSnapshot{
			{
				ExternalProjectID: "proj-1",
				Title:             "Test Project",
				Worktree:          "/tmp/test",
			},
		},
		ProjectDirectorySnapshots: []ProjectDirectorySnapshot{
			{
				ExternalProjectID: "proj-1",
				Path:              "/tmp/test/src",
			},
		},
		TodoSnapshots: []TodoSnapshot{
			{
				ExternalSessionID: "sess-1",
				Description:       "Write tests",
				Status:            "pending",
			},
		},
	}

	// Marshal to JSON and back.
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded IngestRequest
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(decoded.SessionContexts) != 1 {
		t.Fatalf("expected 1 session context, got %d", len(decoded.SessionContexts))
	}
	if decoded.SessionContexts[0].ExternalSessionID != "sess-1" {
		t.Errorf("session context mismatch")
	}

	if len(decoded.ProjectSnapshots) != 1 {
		t.Fatalf("expected 1 project snapshot, got %d", len(decoded.ProjectSnapshots))
	}
	if decoded.ProjectSnapshots[0].ExternalProjectID != "proj-1" {
		t.Errorf("project snapshot mismatch")
	}

	if len(decoded.ProjectDirectorySnapshots) != 1 {
		t.Fatalf("expected 1 project directory snapshot, got %d", len(decoded.ProjectDirectorySnapshots))
	}

	if len(decoded.TodoSnapshots) != 1 {
		t.Fatalf("expected 1 todo snapshot, got %d", len(decoded.TodoSnapshots))
	}
}

func TestIngestRequest_JSONSerialization_EmptyProjections(t *testing.T) {
	req := IngestRequest{
		SchemaVersion:    "1.1",
		CollectorVersion: "0.1.0",
		ClientHostname:   "test-host",
		SourceDatabaseID: "db-1",
		Records:          []IngestRecord{},
	}

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Verify projection fields are omitted when empty.
	raw := string(body)
	for _, field := range []string{"session_contexts", "project_snapshots", "project_directory_snapshots", "todo_snapshots"} {
		if strings.Contains(raw, field) {
			t.Errorf("field %q should be omitted when empty, but found in: %s", field, raw)
		}
	}

	var decoded IngestRequest
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if decoded.SessionContexts != nil {
		t.Errorf("SessionContexts should be nil when absent from JSON")
	}
}

