-- One row per check per run. Keeping the history means a regression can be traced to the
-- run that introduced it, rather than only being visible while it is still failing.
CREATE TABLE IF NOT EXISTS meta.quality_check (
    run_id text NOT NULL,
    check_name text NOT NULL,
    layer text NOT NULL,
    severity text NOT NULL,
    observed numeric NOT NULL,
    expected text NOT NULL,
    passed boolean NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT quality_check_pkey PRIMARY KEY (run_id, check_name),
    CONSTRAINT quality_check_severity_check CHECK (severity IN ('block', 'warn'))
);

-- Failures are the rare case and the only case anyone searches for, so the index covers
-- them alone instead of the whole history.
CREATE INDEX IF NOT EXISTS quality_check_failed_idx
    ON meta.quality_check (check_name, checked_at DESC)
    WHERE NOT passed;

-- The newest result for every check, whichever run happened to produce it.
CREATE OR REPLACE VIEW meta.v_quality_latest AS
SELECT DISTINCT ON (check_name)
    check_name,
    layer,
    severity,
    observed,
    expected,
    passed,
    run_id,
    checked_at
FROM meta.quality_check
ORDER BY check_name, checked_at DESC;

COMMENT ON TABLE meta.quality_check IS 'Result history of the quality gate, one row per check per run';
COMMENT ON COLUMN meta.quality_check.severity IS 'block fails the task and stops the DAG, warn is recorded and logged only';
