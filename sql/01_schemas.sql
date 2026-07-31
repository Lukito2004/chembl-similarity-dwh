-- Medallion layout. bronze/silver/gold hold data, meta holds pipeline state so
-- operational tables are never mistaken for landed data.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

COMMENT ON SCHEMA bronze IS 'Immutable landing of the ChEMBL API payload, source-shaped, no business logic';
COMMENT ON SCHEMA silver IS 'Typed, deduplicated and conformed molecules';
COMMENT ON SCHEMA gold IS 'Star schema data mart and its delivery views';
COMMENT ON SCHEMA meta IS 'Pipeline state such as ingest watermarks and run history';
