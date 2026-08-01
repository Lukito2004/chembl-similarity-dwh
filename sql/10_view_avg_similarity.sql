-- Task 7a: mean similarity of each source molecule, over its top ten matches.
CREATE OR REPLACE VIEW gold.v_avg_similarity_per_source AS
SELECT
    source_chembl_id,
    avg(tanimoto_score) AS avg_tanimoto_score
FROM gold.fact_molecule_similarity
GROUP BY source_chembl_id;

COMMENT ON VIEW gold.v_avg_similarity_per_source IS 'Task 7a: mean Tanimoto score across each source molecule top ten matches';
