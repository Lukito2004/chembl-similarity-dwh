-- Task 8c: four grouping sets in one pass, with no UNION anywhere.
-- grouping() separates a null the rollup produced from a null already in the data,
-- so only the former is replaced by TOTAL.
CREATE OR REPLACE VIEW gold.v_avg_similarity_rollup AS
SELECT
    CASE WHEN grouping(f.source_chembl_id) = 0 THEN f.source_chembl_id ELSE 'TOTAL' END
        AS source_molecule,
    CASE WHEN grouping(d.aromatic_rings) = 0 THEN d.aromatic_rings::text ELSE 'TOTAL' END
        AS aromatic_rings,
    CASE WHEN grouping(d.heavy_atoms) = 0 THEN d.heavy_atoms::text ELSE 'TOTAL' END
        AS heavy_atoms,
    avg(f.tanimoto_score) AS avg_tanimoto_score
FROM gold.fact_molecule_similarity f
JOIN gold.dim_molecule d ON d.chembl_id = f.source_chembl_id
GROUP BY GROUPING SETS (
    (f.source_chembl_id),
    (d.aromatic_rings, d.heavy_atoms),
    (d.heavy_atoms),
    ()
);

COMMENT ON VIEW gold.v_avg_similarity_rollup IS 'Task 8c: averages by source, by rings and atoms, by atoms, and overall';
