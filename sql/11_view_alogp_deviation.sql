-- Task 7b: mean absolute deviation of alogp between a source and each of its matches.
-- Pairs where either alogp is null contribute nothing, because avg skips nulls.
CREATE OR REPLACE VIEW gold.v_avg_alogp_deviation AS
SELECT
    f.source_chembl_id,
    avg(abs(t.alogp - s.alogp)) AS avg_alogp_deviation
FROM gold.fact_molecule_similarity f
JOIN gold.dim_molecule s ON s.chembl_id = f.source_chembl_id
JOIN gold.dim_molecule t ON t.chembl_id = f.target_chembl_id
GROUP BY f.source_chembl_id;

COMMENT ON VIEW gold.v_avg_alogp_deviation IS 'Task 7b: mean absolute alogp deviation of each match from its source';
