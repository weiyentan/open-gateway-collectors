# ADR-0004: Cursor-based incremental reading

The collector tracks the last-sent `message.time_updated` timestamp per source database in a local state file (`.collector-state`) so that after a restart it resumes from where it left off rather than re-scanning all historic messages — this keeps each push cycle fast, avoids redundant ingest of already-deduplicated records, and naturally supports backfill on first run (cursor starts at zero).
