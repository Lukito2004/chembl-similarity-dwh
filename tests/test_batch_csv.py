from chembl_sim.inputs import batch_csv

STANDARD = (
    "compound_id,compound_name,molecular_weight,logp,ic50_nm,assay_date,lab_id\n"
    "CPD-001,Aspirin,180.16,1.19,2500.0,2024-01-15,LAB-A\n"
    "CPD-002,Ibuprofen,206.28,3.97,1200.0,2024-01-15,LAB-A\n"
)

NO_LOGP = (
    "compound_id,compound_name,molecular_weight,ic50_nm,assay_date,lab_id\n"
    "CPD-043,,284.74,920.0,2024-01-23,LAB-F\n"
)

MIXED_CASE = (
    "compound_id,compound_name,molecular_weight,logp,IC50_nM,collection_date,lab_id\n"
    "CPD-051,Escitalopram,324.39,3.86,145.0,2024-01-25,LAB-G\n"
)


def column(row, name):
    return row[batch_csv.BRONZE_COLUMNS.index(name)]


def test_rows_carry_their_file_and_line_number():
    rows = batch_csv.parse_batch_csv(STANDARD, "batch_001.csv").rows
    assert [(r[0], r[1]) for r in rows] == [("batch_001.csv", 2), ("batch_001.csv", 3)]


def test_values_land_in_the_bronze_column_order():
    row = batch_csv.parse_batch_csv(STANDARD, "batch_001.csv").rows[0]
    assert column(row, "compound_id") == "CPD-001"
    assert column(row, "compound_name") == "Aspirin"
    assert column(row, "logp") == "1.19"
    assert column(row, "collection_date") is None


def test_a_missing_column_becomes_none():
    row = batch_csv.parse_batch_csv(NO_LOGP, "batch_004.csv").rows[0]
    assert column(row, "logp") is None
    assert column(row, "molecular_weight") == "284.74"


def test_a_blank_compound_name_is_kept_as_none():
    row = batch_csv.parse_batch_csv(NO_LOGP, "batch_004.csv").rows[0]
    assert column(row, "compound_name") is None


def test_header_case_is_normalised():
    row = batch_csv.parse_batch_csv(MIXED_CASE, "batch_005.csv").rows[0]
    assert column(row, "ic50_nm") == "145.0"


def test_both_date_columns_are_supported():
    row = batch_csv.parse_batch_csv(MIXED_CASE, "batch_005.csv").rows[0]
    assert column(row, "collection_date") == "2024-01-25"
    assert column(row, "assay_date") is None


def test_blank_lines_are_skipped():
    parsed = batch_csv.parse_batch_csv(STANDARD + "\n,,,,,,\n", "batch_001.csv")
    assert len(parsed.rows) == 2


def test_unknown_headers_are_reported_not_dropped_silently():
    parsed = batch_csv.parse_batch_csv("compound_id,mystery\nCPD-1,x\n", "odd.csv")
    assert parsed.unknown_headers == {"mystery"}


def test_an_empty_file_yields_nothing():
    assert batch_csv.parse_batch_csv("", "empty.csv").rows == []


def test_short_rows_do_not_raise():
    parsed = batch_csv.parse_batch_csv(
        "compound_id,compound_name,lab_id\nCPD-1,Aspirin\n", "ragged.csv"
    )
    assert column(parsed.rows[0], "lab_id") is None
