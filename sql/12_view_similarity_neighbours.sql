-- Task 8b: every match with the next one down for the same source, plus that source's
-- own second best match. Ties break on the identifier so the ordering is repeatable.
-- nth_value needs the whole partition in frame; the default frame stops at the current row.
CREATE OR REPLACE VIEW gold.v_similarity_neighbours AS
SELECT
    source_chembl_id,
    target_chembl_id,
    tanimoto_score,
    lead(target_chembl_id) OVER ranked AS next_most_similar_target,
    nth_value(target_chembl_id, 2) OVER whole_partition AS second_most_similar_target
FROM gold.fact_molecule_similarity
WINDOW
    ranked AS (PARTITION BY source_chembl_id ORDER BY tanimoto_score DESC, target_chembl_id),
    whole_partition AS (ranked ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

COMMENT ON VIEW gold.v_similarity_neighbours IS 'Task 8b: next match down and the source second best match, in one query';
