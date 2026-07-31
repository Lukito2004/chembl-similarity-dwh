-- Resumable ingest progress. A multi-hour API ingest will be interrupted, so each
-- window of pages records where the next run should restart.
CREATE TABLE IF NOT EXISTS meta.ingest_watermark (
    resource text PRIMARY KEY,
    chembl_release text NOT NULL,
    next_offset integer NOT NULL DEFAULT 0,
    total_count integer,
    is_complete boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);
