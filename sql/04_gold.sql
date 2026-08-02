-- Dimension columns and their order follow the task specification exactly.
CREATE TABLE IF NOT EXISTS gold.dim_molecule (
    chembl_id text PRIMARY KEY,
    molecule_type text,
    mw_freebase numeric,
    alogp numeric,
    psa numeric,
    cx_logp numeric,
    molecular_species text,
    full_mwt numeric,
    aromatic_rings integer,
    heavy_atoms integer,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.fact_molecule_similarity (
    source_chembl_id text NOT NULL,
    target_chembl_id text NOT NULL,
    tanimoto_score numeric(9, 8) NOT NULL,
    has_duplicates_of_last_largest_score boolean NOT NULL DEFAULT false,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fact_molecule_similarity_pkey
        PRIMARY KEY (source_chembl_id, target_chembl_id),
    CONSTRAINT fact_molecule_similarity_source_fkey
        FOREIGN KEY (source_chembl_id) REFERENCES gold.dim_molecule (chembl_id),
    CONSTRAINT fact_molecule_similarity_target_fkey
        FOREIGN KEY (target_chembl_id) REFERENCES gold.dim_molecule (chembl_id),
    CONSTRAINT fact_molecule_similarity_score_range
        CHECK (tanimoto_score >= 0 AND tanimoto_score <= 1)
);

-- The primary key already serves lookups by source. Views 8a and 8b also group by
-- target, which needs its own index.
CREATE INDEX IF NOT EXISTS fact_molecule_similarity_target_idx
    ON gold.fact_molecule_similarity (target_chembl_id);

COMMENT ON TABLE gold.dim_molecule IS 'Restricted to molecules referenced by the fact table';
COMMENT ON COLUMN gold.dim_molecule.cx_logp IS 'Always null, the field was removed from ChEMBL before release 37';
COMMENT ON COLUMN gold.dim_molecule.molecular_species IS 'Always null, the field was removed from ChEMBL before release 37';
COMMENT ON COLUMN gold.fact_molecule_similarity.has_duplicates_of_last_largest_score IS 'Set on top-10 rows tied with the tenth score when molecules outside the top 10 share it';
