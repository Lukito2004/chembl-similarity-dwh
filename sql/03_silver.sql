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

COMMENT ON COLUMN silver.molecule.cx_logp IS 'Not present in ChEMBL 37, kept as a landing spot for an older-release backfill';
COMMENT ON COLUMN silver.molecule.molecular_species IS 'Not present in ChEMBL 37, kept as a landing spot for an older-release backfill';
