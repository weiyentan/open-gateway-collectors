// Package collector provides the main orchestration loop that discovers
// OpenCode source databases, reads usage records, POSTs them to the Gateway,
// and sends heartbeats when idle.
package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/opencode-gateway/collectors/internal/config"
	"github.com/opencode-gateway/collectors/internal/exclusion"
	"github.com/opencode-gateway/collectors/internal/gateway"
	"github.com/opencode-gateway/collectors/internal/heartbeat"
	"github.com/opencode-gateway/collectors/internal/identity"
	"github.com/opencode-gateway/collectors/internal/sqlite"
	"github.com/opencode-gateway/collectors/internal/state"
)

// readerFactory creates a sqlite.Reader for the given database path along
// with a close function. The dbInfo parameter carries schema detection
// results from OpenAndInspect so the reader can be schema-aware.
// Injected for testability.
type readerFactory func(dbPath string, dbInfo *sqlite.DatabaseInfo) (sqlite.Reader, func(), error)

// Collector orchestrates the periodic discovery, reading, and pushing of
// usage records from OpenCode source databases to the Gateway.
type Collector struct {
	cfg           *config.Config
	transport     gateway.Transport
	tracker       *state.Tracker
	identityStore *identity.Store
	exclusionGate *exclusion.Gate
	logger        *slog.Logger
	hostname      string
	version       string

	mu          sync.Mutex
	lastSuccess map[string]time.Time // keyed by source database path
	batchLimit  int
	seedOnce    sync.Once

	newReader readerFactory

	// replaySince is the computed effective since time for replay mode.
	// Zero time means full history. Only meaningful when cfg.Replay is true.
	replaySince time.Time

	// replayCompleted tracks databases that have completed replay mode.
	// In replay mode, each database is replayed once per process lifetime.
	// The replay pass is skipped on subsequent poll cycles after completion.
	replayCompleted map[string]bool
}

// dbIdentity holds the resolved identity for a single discovered database.
type dbIdentity struct {
	path   string
	id     string // UUID string from identity store
	dbInfo *sqlite.DatabaseInfo
}

// NewCollector wires all components together and returns a Collector ready
// to run. It resolves the client hostname once at startup via os.Hostname
// and selects the transport (HTTP or Kafka) based on config.Transport.
// Startup details are logged at info level — the bearer token is never logged.
func NewCollector(cfg *config.Config, version string) (*Collector, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resolving hostname: %w", err)
	}

	logger := newLogger(cfg.LogLevel)

	logger.Info("collector starting",
		"version", version,
		"hostname", hostname,
		"transport", cfg.Transport,
		"base_url", cfg.BaseURL,
		"poll_interval", cfg.PollInterval.String(),
		"heartbeat_interval", cfg.HeartbeatInterval.String(),
		"batch_limit", cfg.BatchLimit,
		"log_level", cfg.LogLevel,
	)

	// Compute the replay since time from config. Zero ReplaySince means full
	// history (zero time). A non-zero value means replay records newer than
	// time.Now().Add(-ReplaySince).
	var replaySince time.Time
	if cfg.Replay && cfg.ReplaySince > 0 {
		replaySince = time.Now().Add(-cfg.ReplaySince)
	}

	// Log replay configuration if enabled.
	if cfg.Replay {
		if cfg.ReplaySince > 0 {
			logger.Info("replay mode enabled",
				"replay_since", replaySince.Format(time.RFC3339),
				"replay_since_duration", cfg.ReplaySince.String(),
			)
		} else {
			logger.Info("replay mode enabled — full history")
		}
	}

	tracker, err := state.NewTracker(cfg.CursorDir)
	if err != nil {
		return nil, fmt.Errorf("creating state tracker: %w", err)
	}

	var transport gateway.Transport
	switch cfg.Transport {
	case "http":
		transport = gateway.NewClient(cfg.BaseURL, cfg.Token, hostname)
	case "kafka":
		transport, err = gateway.NewKafkaClient(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaClientID, hostname)
		if err != nil {
			return nil, fmt.Errorf("create kafka client: %w", err)
		}
	default:
		// Default to HTTP for backward compatibility with tests and embedded
		// use where Transport is unset (zero value).
		transport = gateway.NewClient(cfg.BaseURL, cfg.Token, hostname)
	}

	return &Collector{
		cfg:             cfg,
		transport:       transport,
		tracker:         tracker,
		identityStore:   identity.NewStore(cfg.CursorDir),
		exclusionGate:   exclusion.NewGate(cfg.CursorDir, cfg.ExcludeRecheckInterval),
		logger:          logger,
		hostname:        hostname,
		version:         version,
		lastSuccess:     make(map[string]time.Time),
		batchLimit:      cfg.BatchLimit,
		newReader:       defaultReaderFactory,
		replaySince:     replaySince,
		replayCompleted: make(map[string]bool),
	}, nil
}

// defaultReaderFactory opens an OpenCodeReader in read-only mode and attaches
// the DatabaseInfo for schema-aware projection reading.
func defaultReaderFactory(dbPath string, dbInfo *sqlite.DatabaseInfo) (sqlite.Reader, func(), error) {
	reader, err := sqlite.NewOpenCodeReader(dbPath)
	if err != nil {
		return nil, nil, err
	}
	reader.WithSchemaInfo(dbInfo)
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
			// Close transport if it supports io.Closer (e.g., KafkaClient).
			if closer, ok := c.transport.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					c.logger.Error("closing transport", "error", err)
				}
			}
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

	// Seed lastSuccess for databases with persisted cursors so that
	// heartbeats work immediately after restart for databases that
	// were previously tracked. This runs exactly once at startup to
	// avoid redundant GetCursor calls on every poll cycle.
	c.seedOnce.Do(func() {
		for _, db := range dbs {
			cursor, err := c.tracker.GetCursor(db.path)
			if err != nil {
				c.logger.Warn("failed to get cursor for lastSuccess seeding",
					"path", db.path,
					"error", err,
				)
				continue
			}
			if !cursor.IsZero() {
				c.mu.Lock()
				if _, exists := c.lastSuccess[db.path]; !exists {
					// Use time.Now() rather than the cursor timestamp because
					// "last success" represents when the collector last sent
					// data. On restart the most recent successful data send
					// was just now (the collector started successfully).
					// Using the cursor timestamp would make the heartbeat
					// interval immediately expire, causing a heartbeat burst.
					c.lastSuccess[db.path] = time.Now()
				}
				c.mu.Unlock()
			}
		}
	})

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
// scans SQLiteDir for .db files. Excluded databases whose recheck is not due
// are skipped without opening. Candidates that fail inspection are excluded
// via the gate and skipped. Databases that pass a recheck are re-admitted
// and processed normally. Identity is resolved (or created) for each valid
// database.
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
		// Gate check — fast path: skip excluded DBs whose recheck isn't due.
		excluded := false
		excludedBool, err := c.exclusionGate.IsExcluded(path)
		if err != nil {
			c.logger.Warn("exclusion check failed — proceeding with inspection",
				"path", path,
				"error", err,
			)
		} else {
			excluded = excludedBool
			if excluded {
				due, err := c.exclusionGate.RecheckDue(path)
				if err != nil {
					c.logger.Warn("recheck check failed — proceeding with re-inspection",
						"path", path,
						"error", err,
					)
				} else if !due {
					continue // skip without opening the file
				}
				// Recheck is due — fall through to OpenAndInspect.
			}
		}

		dbInfo, err := sqlite.OpenAndInspect(path)
		if err != nil {
			c.logger.Warn("gate failed — excluding database",
				"path", path,
				"error", err,
			)
			if exclErr := c.exclusionGate.Exclude(path, err.Error()); exclErr != nil {
				c.logger.Warn("failed to persist exclusion",
					"path", path,
					"error", exclErr,
				)
			}
			continue
		}

		// Passed inspection — if was previously excluded, re-admit.
		if excluded {
			c.logger.Info("database re-admitted after passing recheck",
				"path", path,
			)
			if rmErr := c.exclusionGate.Remove(path); rmErr != nil {
				c.logger.Warn("failed to remove exclusion after re-admission",
					"path", path,
					"error", rmErr,
				)
			}
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
//
// In replay mode (cfg.Replay == true), the effective since time is
// replaySince (which may be zero for full history) instead of the stored
// cursor, and processDatabase loops to read and send all matching records
// in batches up to batchLimit. The persisted cursor is only updated once
// the full replay pass completes — per-batch cursor persistence is
// deferred to avoid leaving the cursor inside a tie group if a later batch
// sharing the same timestamp fails (Comment E Finding 1). At completion
// the persisted cursor is clamped: if the replay window starts after the
// stored cursor, the cursor stays at the stored cursor so records in
// (storedCursor, replaySince] (never ingested, not re-read) remain
// readable by normal incremental mode, and the completion cursor never
// regresses below the stored cursor. On a replay failure, the cursor is
// rewound to replaySince so a subsequent restart (even without replay)
// re-reads the entire window; a rewind SetCursor failure is logged so an
// operator can intervene before records are skipped.
//
// The replay pass is one-shot: once replay completes for a database, the
// replayCompleted flag prevents re-triggering on subsequent poll cycles.
// During replay, a failed batch POST does NOT advance the read window —
// the failed records are retried on the next poll cycle, consistent with
// normal mode behavior.
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

	// Determine whether to run a replay pass for this database.
	// Replay is only entered once per database per process lifetime.
	useReplay := c.cfg.Replay && !c.replayCompleted[db.path]

	// Determine the effective since time. In replay mode, the stored cursor
	// is ignored — we re-read from replaySince (full history if zero).
	effectiveSince := cursor
	if useReplay {
		effectiveSince = c.replaySince
		logger.Info("replay active — reading from effective since",
			"effective_since", effectiveSince.Format(time.RFC3339),
			"stored_cursor", cursor.Format(time.RFC3339),
		)
	}

	reader, closeFn, err := c.newReader(db.path, db.dbInfo)
	if err != nil {
		logger.Error("failed to open reader", "error", err)
		return
	}
	defer closeFn()

	// effectiveLastID is the secondary key for tie-safe composite paging.
	// When non-empty, subsequent pages use ReadRecordsAfter with the
	// composite (time_updated, id) cursor to avoid dropping records that
	// share a timestamp with the last record of the previous page.
	var effectiveLastID string

	// Replay loop: in replay mode, keep reading batches until no more
	// records are returned. In normal mode, this runs exactly once.
	for {
		var records []sqlite.UsageRecord
		var err error
		if useReplay && effectiveLastID != "" {
			records, err = reader.ReadRecordsAfter(effectiveSince, effectiveLastID, c.batchLimit)
		} else {
			records, err = reader.ReadRecords(effectiveSince, c.batchLimit)
		}
		if err != nil {
			logger.Error("failed to read records", "error", err)
			return
		}

		if len(records) == 0 {
			if useReplay {
				// Replay complete — no more records. Persist cursor at
				// the effective since time (last batch's max, or
				// replaySince if no batches were sent) so normal
				// incremental mode resumes from the correct position.
				// The cursor is clamped so it never skips records that
				// were never ingested and never regresses below the
				// pre-replay stored cursor: if the replay window starts
				// after the stored cursor, records in
				// (storedCursor, replaySince] were neither previously
				// ingested nor re-read by the replay — keeping the
				// cursor at the stored cursor leaves them readable by
				// normal incremental mode.
				// Mark replay done so subsequent poll cycles skip replay.
				finalCursor := effectiveSince
				if c.replaySince.After(cursor) {
					finalCursor = cursor
				} else if finalCursor.Before(cursor) {
					finalCursor = cursor // never regress
				}
				_ = c.tracker.SetCursor(db.path, finalCursor)
				c.replayCompleted[db.path] = true
			}
			// Send heartbeat regardless of mode so zero-record databases
			// still get a heartbeat on the replay-complete pass.
			// maybeSendHeartbeat gates internally on prior success + interval.
			c.maybeSendHeartbeat(ctx, db, logger)
			return
		}

		// Extract unique session IDs and project IDs from the records
		// for projection reads.
		sessionIDs := uniqueSessionIDs(records)
		projectIDs := uniqueProjectIDs(records)

		sessionCtxs, _ := reader.ReadSessionContexts(sessionIDs)
		projects, _ := reader.ReadProjectData(projectIDs)
		projectDirs, _ := reader.ReadProjectDirectoryData(projectIDs)
		todos, _ := reader.ReadTodoData(sessionIDs)

		if err := c.sendRecords(ctx, db, records, sessionCtxs, projects, projectDirs, todos, logger, !useReplay); err != nil {
			// Batch send failed — cursor was NOT updated by sendRecords.
			// In replay mode, rewind the persisted cursor to replaySince
			// so a subsequent restart (even without replay) re-reads the
			// full window. Rewinding is safe: the earliest batch(s) that
			// succeeded will be re-sent idempotently. In normal mode,
			// returning without advancing is correct (cursor unchanged).
			// The rewind error must be surfaced: if the rewind write
			// fails, the persisted cursor stays at its pre-replay
			// position — typically AHEAD of the failed batch — and a
			// restart without replay would permanently skip those
			// records (normal mode propagates SetCursor failures, so
			// replay should not be weaker).
			if useReplay {
				if err := c.tracker.SetCursor(db.path, c.replaySince); err != nil {
					logger.Error("failed to rewind cursor after replay failure; restart without replay may skip the failed batch", "error", err)
				}
			}
			return
		}

		// In replay mode, advance the composite cursor to the last
		// record's (time_updated, id) and loop if the batch was full
		// (more records may exist). In normal mode, exit after one batch.
		if !useReplay {
			return
		}

		// If fewer records than the batch limit were returned, we've read
		// everything — done with replay for this database.
		if len(records) < c.batchLimit {
			logger.Info("replay complete for database",
				"records_sent_in_final_batch", len(records),
			)
			// Persist cursor at the final batch's max occurred_at so
			// subsequent normal incremental mode starts after all
			// replayed records. The cursor is clamped so it never skips
			// records that were never ingested and never regresses below
			// the pre-replay stored cursor: if the replay window starts
			// after the stored cursor, records in
			// (storedCursor, replaySince] were neither previously
			// ingested nor re-read by the replay — keeping the cursor at
			// the stored cursor leaves them readable by normal
			// incremental mode.
			finalCursor := lastRecord(records).OccurredAt
			if c.replaySince.After(cursor) {
				finalCursor = cursor
			} else if finalCursor.Before(cursor) {
				finalCursor = cursor // never regress
			}
			_ = c.tracker.SetCursor(db.path, finalCursor)
			c.replayCompleted[db.path] = true
			return
		}

		// Advance the composite cursor for tie-safe paging. Records are
		// ordered ASC by (time_updated, id), so the last element is the
		// composite max. Setting both effectiveSince and effectiveLastID
		// ensures the next page starts strictly after the last record,
		// preventing silent drops at tied timestamps.
		last := lastRecord(records)
		effectiveSince = last.OccurredAt
		effectiveLastID = last.SourceRecordID
	}
}

// sendRecords converts sqlite usage records to ingest records, builds an
// IngestRequest with batch-level projection snapshots, POSTs it to the
// Gateway, and updates the cursor on success (when persistCursor is true).
// On failure, the cursor is NOT updated and a non-nil error is returned —
// the same records will be retried on the next iteration.
//
// persistCursor controls whether the persisted cursor is advanced to the
// maximum occurred_at timestamp in the batch. During replay, the caller
// passes false so cursor persistence is deferred to replay completion
// (avoiding permanent data loss if a later batch in the same tie group
// fails). In normal incremental mode, the caller passes true for
// immediate per-batch persistence.
func (c *Collector) sendRecords(
	ctx context.Context,
	db dbIdentity,
	records []sqlite.UsageRecord,
	sessionCtxs []sqlite.SessionContextData,
	projects []sqlite.ProjectData,
	projectDirs []sqlite.ProjectDirectoryData,
	todos []sqlite.TodoData,
	logger *slog.Logger,
	persistCursor bool,
) error {
	ingestRecords := make([]gateway.IngestRecord, 0, len(records))
	for i := range records {
		gwRec := ToGatewayUsageRecord(records[i])
		ingestRecords = append(ingestRecords, gateway.MapToIngestRecord(gwRec))
	}

	// Map projections to wire types with de-duplication.
	reqSessionCtxs := dedupSessionContexts(sessionCtxs)
	reqProjects := dedupProjectSnapshots(projects)
	reqProjectDirs := dedupProjectDirectorySnapshots(projectDirs)
	reqTodos := dedupTodoSnapshots(todos)

	req := &gateway.IngestRequest{
		SchemaVersion:     gateway.SchemaVersion,
		CollectorVersion:  c.version,
		SourceDatabaseID:  db.id,
		Records:           ingestRecords,
		SessionContexts:   reqSessionCtxs,
		Projects:          reqProjects,
		ProjectDirectories: reqProjectDirs,
		SessionTodos:      reqTodos,
	}

	resp, err := c.transport.SendBatch(ctx, req)
	if err != nil {
		logger.Error("batch send failed — cursor not updated",
			"error", err,
			"record_count", len(records),
		)
		return fmt.Errorf("send batch: %w", err)
	}

	// Persist cursor per-batch only outside of replay mode (normal
	// incremental operation). During replay, the cursor is only persisted
	// once the full replay pass completes — persisting per-batch during
	// replay risks leaving the cursor inside a tie group if a later batch
	// sharing the same timestamp fails (Comment E Finding 1).
	// Find max occurred_at among sent records (records are ordered ASC by
	// (time_updated, id), so the last element is the composite max).
	maxOccurred := lastRecord(records).OccurredAt
	if persistCursor {
		if err := c.tracker.SetCursor(db.path, maxOccurred); err != nil {
			logger.Error("failed to update cursor after successful send",
				"error", err,
			)
			return fmt.Errorf("set cursor: %w", err)
		}
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
		"session_contexts", len(reqSessionCtxs),
		"project_snapshots", len(reqProjects),
		"project_directory_snapshots", len(reqProjectDirs),
		"todo_snapshots", len(reqTodos),
	)
	return nil
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

	resp, err := c.transport.SendBatch(ctx, req)
	if err != nil {
		logger.Warn("heartbeat send failed", "error", err)
		return
	}

	c.mu.Lock()
	c.lastSuccess[db.path] = time.Now()
	c.mu.Unlock()

	logger.Info("heartbeat sent", "batch_id", resp.BatchID)
}

// ToGatewayUsageRecord converts a sqlite.UsageRecord to the gateway
// package's UsageRecord type for use with MapToIngestRecord.
func ToGatewayUsageRecord(rec sqlite.UsageRecord) gateway.UsageRecord {
	return gateway.UsageRecord{
		SourceRecordID:   rec.SourceRecordID,
		SessionID:        rec.SourceSessionID,
		Model:            rec.ModelID,
		ProviderID:       rec.ProviderID,
		Mode:             rec.Mode,
		Agent:            rec.Agent,
		ProjectID:        rec.SourceProjectID,
		WorkspaceID:      rec.WorkspaceID,
		ParentSessionID:  rec.ParentSessionID,
		ReasoningTokens:  rec.TokensReasoning,
		FinishReason:     rec.FinishReason,
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

// ---------------------------------------------------------------------------
// General helpers
// ---------------------------------------------------------------------------

// lastRecord returns the last record in a batch ordered ascending by
// (time_updated, id). It is the composite maximum (time_updated, id) of the
// batch and defines the exclusive boundary for the next page read.
func lastRecord(records []sqlite.UsageRecord) sqlite.UsageRecord {
	return records[len(records)-1]
}

// ---------------------------------------------------------------------------
// Projection helper functions
// ---------------------------------------------------------------------------

// uniqueSessionIDs extracts distinct, non-empty session IDs from usage
// records for projection reading.
func uniqueSessionIDs(records []sqlite.UsageRecord) []string {
	seen := make(map[string]bool)
	var result []string
	for _, rec := range records {
		if rec.SourceSessionID != "" && !seen[rec.SourceSessionID] {
			seen[rec.SourceSessionID] = true
			result = append(result, rec.SourceSessionID)
		}
	}
	return result
}

// uniqueProjectIDs extracts distinct, non-empty project IDs from usage
// records for projection reading.
func uniqueProjectIDs(records []sqlite.UsageRecord) []string {
	seen := make(map[string]bool)
	var result []string
	for _, rec := range records {
		if rec.SourceProjectID != "" && !seen[rec.SourceProjectID] {
			seen[rec.SourceProjectID] = true
			result = append(result, rec.SourceProjectID)
		}
	}
	return result
}

// dedupSessionContexts de-duplicates SessionContextData by ExternalSessionID
// within a batch and maps to wire types.
func dedupSessionContexts(data []sqlite.SessionContextData) []gateway.SessionContext {
	seen := make(map[string]bool)
	var result []gateway.SessionContext
	for _, d := range data {
		if d.ExternalSessionID != "" && !seen[d.ExternalSessionID] {
			seen[d.ExternalSessionID] = true
			result = append(result, gateway.MapToSessionContext(d))
		}
	}
	return result
}

// dedupProjectSnapshots de-duplicates ProjectData by ExternalProjectID within
// a batch and maps to wire types.
func dedupProjectSnapshots(data []sqlite.ProjectData) []gateway.ProjectSnapshot {
	seen := make(map[string]bool)
	var result []gateway.ProjectSnapshot
	for _, d := range data {
		if d.ExternalProjectID != "" && !seen[d.ExternalProjectID] {
			seen[d.ExternalProjectID] = true
			result = append(result, gateway.MapToProjectSnapshot(d))
		}
	}
	return result
}

// dedupProjectDirectorySnapshots de-duplicates ProjectDirectoryData by
// ExternalProjectID within a batch and maps to wire types.
func dedupProjectDirectorySnapshots(data []sqlite.ProjectDirectoryData) []gateway.ProjectDirectorySnapshot {
	seen := make(map[string]bool)
	var result []gateway.ProjectDirectorySnapshot
	for _, d := range data {
		key := d.ExternalProjectID + "\x00" + d.Path
		if d.ExternalProjectID != "" && !seen[key] {
			seen[key] = true
			result = append(result, gateway.MapToProjectDirectorySnapshot(d))
		}
	}
	return result
}

// dedupTodoSnapshots de-duplicates TodoData by combination of
// ExternalSessionID and Description within a batch and maps to wire types.
func dedupTodoSnapshots(data []sqlite.TodoData) []gateway.TodoSnapshot {
	seen := make(map[string]bool)
	var result []gateway.TodoSnapshot
	for _, d := range data {
		key := d.ExternalSessionID + "\x00" + d.Description
		if d.ExternalSessionID != "" && !seen[key] {
			seen[key] = true
			result = append(result, gateway.MapToTodoSnapshot(d))
		}
	}
	return result
}
