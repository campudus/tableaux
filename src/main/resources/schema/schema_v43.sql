ALTER TABLE system_link_table ADD COLUMN attributes JSONB;

-- Link-attribute VALUES live per link-row, on each dynamically-created link_table_<linkId>.
-- New link tables get the column via the updated CREATE TABLE DDL in ColumnModel.createLinkColumn.
-- Existing link tables are backfilled here.
CREATE OR REPLACE FUNCTION add_attributes_column_to_link_table(link_table REGCLASS)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE ' || link_table || ' ADD COLUMN IF NOT EXISTS attributes JSONB';
  RETURN link_table :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_attributes_column_to_link_table(table_name :: REGCLASS)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'link_table_%'
ORDER BY table_name;

DROP FUNCTION add_attributes_column_to_link_table(REGCLASS);
