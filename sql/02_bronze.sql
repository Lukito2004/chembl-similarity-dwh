-- Column types mirror what the ChEMBL API sends: decimals arrive as JSON strings
-- and land as text, integers and booleans land as themselves.
CREATE TABLE IF NOT EXISTS bronze.chembl_id_lookup (
    chembl_id text PRIMARY KEY,
    entity_type text,
    entity_id bigint,
    last_active integer,
    resource_url text,
    status text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

-- The API calls the key molecule_chembl_id. It is renamed to chembl_id here so all
-- four bronze tables share one join key.
CREATE TABLE IF NOT EXISTS bronze.molecule_dictionary (
    chembl_id text PRIMARY KEY,
    pref_name text,
    max_phase text,
    molecule_type text,
    structure_type text,
    first_approval integer,
    availability_type integer,
    usan_year integer,
    usan_stem text,
    usan_substem text,
    usan_stem_definition text,
    helm_notation text,
    black_box_warning integer,
    chemical_probe integer,
    chirality integer,
    first_in_class integer,
    inorganic_flag integer,
    natural_product integer,
    orphan integer,
    polymer_flag integer,
    prodrug integer,
    veterinary integer,
    dosed_ingredient boolean,
    oral boolean,
    parenteral boolean,
    topical boolean,
    therapeutic_flag boolean,
    withdrawn_flag boolean,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.compound_properties (
    chembl_id text PRIMARY KEY,
    alogp text,
    psa text,
    mw_freebase text,
    full_mwt text,
    full_molformula text,
    qed_weighted text,
    np_likeness_score text,
    ro3_pass text,
    aromatic_rings integer,
    heavy_atoms integer,
    hba integer,
    hbd integer,
    rtb integer,
    num_ro5_violations integer,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.compound_structures (
    chembl_id text PRIMARY KEY,
    canonical_smiles text,
    standard_inchi text,
    standard_inchi_key text,
    molfile text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE bronze.molecule_dictionary IS 'Scalar top-level fields of the molecule endpoint; nested entities are out of scope';
COMMENT ON COLUMN bronze.molecule_dictionary.max_phase IS 'The API returns this as a string such as 4.0, cast to numeric in silver';
COMMENT ON COLUMN bronze.compound_properties.ro3_pass IS 'The API returns Y or N, cast to boolean in silver';
