package heartbeat

import (
	"testing"
)

func TestBuildHeartbeat_AllFieldsSet(t *testing.T) {
	req := BuildHeartbeat("db-123", "1.0.0", "my-host")

	if req.SchemaVersion != "1.0" {
		t.Errorf("SchemaVersion = %q, want %q", req.SchemaVersion, "1.0")
	}
	if req.CollectorVersion != "1.0.0" {
		t.Errorf("CollectorVersion = %q, want %q", req.CollectorVersion, "1.0.0")
	}
	if req.ClientHostname != "my-host" {
		t.Errorf("ClientHostname = %q, want %q", req.ClientHostname, "my-host")
	}
	if req.SourceDatabaseID != "db-123" {
		t.Errorf("SourceDatabaseID = %q, want %q", req.SourceDatabaseID, "db-123")
	}
}

func TestBuildHeartbeat_EmptyRecords(t *testing.T) {
	req := BuildHeartbeat("db-1", "dev", "host")

	if req.Records == nil {
		t.Fatal("Records is nil, want empty slice")
	}
	if len(req.Records) != 0 {
		t.Errorf("Records length = %d, want 0", len(req.Records))
	}
}

func TestBuildHeartbeat_DifferentSourceIDs(t *testing.T) {
	req1 := BuildHeartbeat("db-alpha", "v1", "h1")
	req2 := BuildHeartbeat("db-beta", "v1", "h2")

	if req1.SourceDatabaseID == req2.SourceDatabaseID {
		t.Error("expected different SourceDatabaseID values")
	}
}
