// Package collector provides the main orchestration loop that discovers
// OpenCode source databases, reads usage records, POSTs them to the Gateway,
// and sends heartbeats when idle.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/opencode-gateway/collectors/internal/config"
	"github.com/opencode-gateway/collectors/internal/gateway"
	"github.com/opencode-gateway/collectors/internal/heartbeat"
	"github.com/opencode-gateway/collectors/internal/identity"
	"github.com/opencode-gateway/collectors/internal/sqlite"
	"github.com/opencode-gateway/collectors/internal/state"
)

const defaultBatchLimit = 500

// readerFactory creates a sqlite.Reader for the given database path along
// with a close function. Injected for testability.
type readerFactory func(dbPath string) (sqlite.Reader, func(), error)

// Collector orchestrates the periodic discovery, reading, and pushing of
// usage records from OpenCode source databases to the Gateway.
type Collector struct {
	cfg           *config.Config
	gwClient      *gateway.Client
	tracker       *state.Tracker
	identityStore *identity.Store
	logger        *slog.Logger
	hostname      string
	version       string

	mu          sync.Mutex
	lastSuccess map[string]time.Time // keyed by source database path
	batchLimit  int

	newReader readerFactory
}

// dbIdentity holds the resolved identity for a single discovered database.
type dbIdentity struct {
	path   string
	id     string // UUID string from identity store
	dbInfo *sqlite.DatabaseInfo
}

// NewCollector wires all components together and returns a Collector ready
// to run. It resolves the client hostname once at startup via os.Hostname
// and passes it to the gateway client. Startup details are logged at info
// level — the bearer token is never logged.
func NewCollector(cfg *config.Config, version string) (*Collector, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resolving hostname: %w", err)
	}

	logger := newLogger(cfg.LogLevel)

	logger.Info("collector starting",
		"version", version,
		"hostname", hostname,
		"base_url", cfg.BaseURL,
		"poll_interval", cfg.PollInterval.String(),
		"heartbeat_interval", cfg.HeartbeatInterval.String(),
		"log_level", cfg.LogLevel,
	)

	tracker, err := state.NewTracker(cfg.CursorDir)
	if err != nil {
		return nil, fmt.Errorf("creating state tracker: %w", err)
	}

	return &Collector{
		cfg:           cfg,
		gwClient:      gateway.NewClient(cfg.BaseURL, cfg.Token, hostname),
		tracker:       tracker,
		identityStore: identity.NewStore(cfg.CursorDir),
		logger:        logger,
		hostname:      hostname,
		version:       version,
		lastSuccess:   make(map[string]time.Time),
		batchLimit:    defaultBatchLimit,
		newReader:     defaultReaderFactory,
	}, nil
}

// defaultReaderFactory opens an OpenCodeReader in read-only mode.
func defaultReaderFactory(dbPath string) (sqlite.Reader, func(), error) {
	reader, err := sqlite.NewOpenCodeReader(dbPath)
	if err != nil {
		return nil, nil, err
	}
	return reader, func() { reader.Close() }, nil
}

// Run starts the main orchestration loop. It blocks until ctx is cancelled.
// Each iteration discovers databases, reads new records, and pushes them to
// the Gateway. Heartbeats are sent for idle databases that have previously
// had at least one successful POST. Use context.WithoutCancel(ctx) as the
// operation context to let in-flight POSTs complete during graceful shutdown.
func (c *Collector) Run(ctx context.Context) error {
	opCtx := context.WithoutCancel(ctx)

	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	// Run an immediate iteration on startup.
	c.iterate(opCtx)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("collector shutting down")
			return ctx.Err()
		case <-ticker.C:
			c.iterate(opCtx)
		}
	}
}

// iterate performs one full scan-and-push cycle. It discovers databases,
// then processes each one sequentially. Errors from individual databases
// are logged and skipped — they do not abort the iteration.
func (c *Collector) iterate(ctx context.Context) {
	dbs, err := c.resolveDatabases()
	if err != nil {
		c.logger.Error("failed to resolve databases", "error", err)
		return
	}

	if len(dbs) == 0 {
		c.logger.Debug("no source databases found")
		return
	}

	for _, db := range dbs {
		// Respect context cancellation between databases.
		if err := ctx.Err(); err != nil {
			return
		}
		c.processDatabase(ctx, db)
	}
}

// resolveDatabases discovers all source database paths and resolves their
// identities. If SQLitePath is set, it uses that single file; otherwise it
// scans SQLiteDir for .db files. Each candidate is opened and inspected —
// databases that fail inspection are skipped with a warning. Identity is
// resolved (or created) for each valid database.
func (c *Collector) resolveDatabases() ([]dbIdentity, error) {
	var paths []string

	if c.cfg.SQLitePath != "" {
		paths = []string{c.cfg.SQLitePath}
	} else {
		var err error
		paths, err = sqlite.DiscoverDatabases(c.cfg.SQLiteDir)
		if err != nil {
			return nil, fmt.Errorf("discovering databases in %s: %w", c.cfg.SQLiteDir, err)
		}
	}

	var dbs []dbIdentity
	for _, path := range paths {
		dbInfo, err := sqlite.OpenAndInspect(path)
		if err != nil {
			c.logger.Warn("skipping source database — inspection failed",
				"path", path,
				"error", err,
			)
			continue
		}

		id, err := c.identityStore.GetOrCreateIdentity(path)
		if err != nil {
			c.logger.Warn("skipping source database — identity resolution failed",
				"path", path,
				"error", err,
			)
			continue
		}

		c.logger.Debug("source database discovered",
			"source_database_id", id.String(),
			"path", path,
			"message_count", dbInfo.MessageCount,
			"schema_version", dbInfo.SchemaVersion,
		)

		dbs = append(dbs, dbIdentity{
			path:   path,
			id:     id.String(),
			dbInfo: dbInfo,
		})
	}

	return dbs, nil
}

// processDatabase handles one source database for the current iteration.
// It reads new records since the last cursor, sends them to the Gateway,
// or sends a heartbeat if the database is idle. The cursor is only updated
// after a successful POST — failed POSTs are retried on the next iteration.
func (c *Collector) processDatabase(ctx context.Context, db dbIdentity) {
	logger := c.logger.With(
		"source_database_id", db.id,
		"client_hostname", c.hostname,
	)

	cursor, err := c.tracker.GetCursor(db.path)
	if err != nil {
		logger.Error("failed to get cursor", "error", err)
		return
	}

	reader, closeFn, err := c.newReader(db.path)
	if err != nil {
		logger.Error("failed to open reader", "error", err)
		return
	}
	defer closeFn()

	records, err := reader.ReadRecords(cursor, c.batchLimit)
	if err != nil {
		logger.Error("failed to read records", "error", err)
		return
	}

	if len(records) > 0 {
		c.sendRecords(ctx, db, records, logger)
	} else {
		c.maybeSendHeartbeat(ctx, db, logger)
	}
}

// sendRecords converts sqlite usage records to ingest records, builds an
// IngestRequest, POSTs it to the Gateway, and updates the cursor on success.
// The cursor is advanced to the maximum occurred_at timestamp in the batch.
// On failure, the cursor is NOT updated — the same records will be retried
// on the next iteration.
func (c *Collector) sendRecords(ctx context.Context, db dbIdentity, records []sqlite.UsageRecord, logger *slog.Logger) {
	ingestRecords := make([]gateway.IngestRecord, 0, len(records))
	for i := range records {
		gwRec := toGatewayUsageRecord(records[i])
		ingestRecords = append(ingestRecords, gateway.MapToIngestRecord(gwRec))
	}

	req := &gateway.IngestRequest{
		SchemaVersion:    "1.0",
		CollectorVersion: c.version,
		SourceDatabaseID: db.id,
		Records:          ingestRecords,
	}

	resp, err := c.gwClient.SendBatch(ctx, req)
	if err != nil {
		logger.Error("batch send failed — cursor not updated",
			"error", err,
			"record_count", len(records),
		)
		return
	}

	// Find max occurred_at among sent records (records are ordered ASC by
	// the reader query, but compute explicitly for safety).
	maxOccurred := records[0].OccurredAt
	for i := 1; i < len(records); i++ {
		if records[i].OccurredAt.After(maxOccurred) {
			maxOccurred = records[i].OccurredAt
		}
	}

	if err := c.tracker.SetCursor(db.path, maxOccurred); err != nil {
		logger.Error("failed to update cursor after successful send",
			"error", err,
		)
		return
	}

	c.mu.Lock()
	c.lastSuccess[db.path] = time.Now()
	c.mu.Unlock()

	logger.Info("batch sent successfully",
		"record_count", len(records),
		"batch_id", resp.BatchID,
		"accepted", resp.AcceptedCount,
		"rejected", resp.RejectedCount,
		"cursor", maxOccurred.Format(time.RFC3339),
	)
}

// maybeSendHeartbeat sends an empty-batch heartbeat if no records are
// available, a previous successful POST has occurred for this database,
// and the heartbeat interval has elapsed. The first successful POST
// requirement prevents backfilling with heartbeats.
func (c *Collector) maybeSendHeartbeat(ctx context.Context, db dbIdentity, logger *slog.Logger) {
	c.mu.Lock()
	lastSuccess, exists := c.lastSuccess[db.path]
	c.mu.Unlock()

	if !exists {
		return
	}

	if time.Since(lastSuccess) < c.cfg.HeartbeatInterval {
		return
	}

	req := heartbeat.BuildHeartbeat(db.id, c.version, c.hostname)

	resp, err := c.gwClient.SendBatch(ctx, req)
	if err != nil {
		logger.Warn("heartbeat send failed", "error", err)
		return
	}

	c.mu.Lock()
	c.lastSuccess[db.path] = time.Now()
	c.mu.Unlock()

	logger.Info("heartbeat sent", "batch_id", resp.BatchID)
}

// toGatewayUsageRecord converts a sqlite.UsageRecord to the gateway
// package's UsageRecord type for use with MapToIngestRecord.
func toGatewayUsageRecord(rec sqlite.UsageRecord) gateway.UsageRecord {
	return gateway.UsageRecord{
		SourceRecordID:   rec.SourceRecordID,
		SessionID:        rec.SourceSessionID,
		Model:            rec.ModelID,
		ProviderID:       rec.ProviderID,
		Mode:             rec.Mode,
		InputTokens:      rec.TokensInput,
		OutputTokens:     rec.TokensOutput,
		TokensCacheRead:  rec.TokensCacheRead,
		TokensCacheWrite: rec.TokensCacheWrite,
		EstimatedCostUSD: rec.OpenCodeReportedCost,
		OccurredAt:       rec.OccurredAt,
	}
}

// newLogger creates a configured slog.Logger using a text handler writing
// to stderr. The level is parsed from the config LogLevel string.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})
	return slog.New(handler)
}
