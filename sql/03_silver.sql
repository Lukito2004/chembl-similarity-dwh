-- One row per ChEMBL molecule that has a structure, with the API's string decimals
-- cast to real numeric types. Molecules without a structure cannot be fingerprinted
-- or scored, so they stop at bronze.
CREATE TABLE IF NOT EXISTS silver.molecule (
    chembl_id text PRIMARY KEY,
    molecule_type text,
    pref_name text,
    max_phase numeric(3, 1),
    structure_type text,
    canonical_smiles text NOT NULL,
    standard_inchi_key text,
    mw_freebase numeric,
    full_mwt numeric,
    alogp numeric,
    psa numeric,
    qed_weighted numeric,
    np_likeness_score numeric,
    aromatic_rings integer,
    heavy_atoms integer,
    hba integer,
    hbd integer,
    rtb integer,
    num_ro5_violations integer,
    ro3_pass boolean,
    cx_logp numeric,
    molecular_species text,
    built_at timestamptz NOT NULL DEFAULT now()
);

-- Bronze keeps the API's text representation, so the cast into silver must never
-- raise. Anything that is not a plain number becomes null.
CREATE OR REPLACE FUNCTION silver.safe_numeric(value text) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN value ~ '^\s*-?\d+(\.\d+)?([eE][-+]?\d+)?\s*$' THEN value::numeric
    END;
$$;

-- The source set the similarity search runs over. Input compounds are matched to
-- ChEMBL by preferred name; the remainder is topped up to the configured size.
CREATE TABLE IF NOT EXISTS silver.source_molecule (
    chembl_id text PRIMARY KEY,
    canonical_smiles text NOT NULL,
    origin text NOT NULL,
    input_compound_id text,
    input_compound_name text,
    selected_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_molecule_origin_check CHECK (origin IN ('input_file', 'top_up'))
);

CREATE TABLE IF NOT EXISTS silver.source_molecule_rejected (
    source_file text NOT NULL,
    source_row integer NOT NULL,
    compound_id text,
    compound_name text,
    reason text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_molecule_rejected_pkey PRIMARY KEY (source_file, source_row)
);

-- Top matches staged before the mart load, so the gold tables are populated by a
-- single set-based statement rather than row by row from Python.
CREATE TABLE IF NOT EXISTS silver.similarity_top (
    source_chembl_id text NOT NULL,
    target_chembl_id text NOT NULL,
    tanimoto_score numeric(9, 8) NOT NULL,
    has_duplicates_of_last_largest_score boolean NOT NULL,
    match_rank integer NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT similarity_top_pkey PRIMARY KEY (source_chembl_id, target_chembl_id)
);

COMMENT ON COLUMN silver.molecule.cx_logp IS 'Not present in ChEMBL 37, kept as a landing spot for an older-release backfill';
COMMENT ON COLUMN silver.molecule.molecular_species IS 'Not present in ChEMBL 37, kept as a landing spot for an older-release backfill';
