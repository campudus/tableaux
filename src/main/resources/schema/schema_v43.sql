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

-- format('%I.%I', ...) instead of a bare table_name cast: resolving an unqualified name to a REGCLASS goes through
-- the connection's search_path, which is not necessarily the schema table_schema just filtered on. BASE TABLE keeps
-- views and foreign tables out - information_schema.tables lists them alongside real tables, and neither can take
-- an ALTER TABLE ... ADD COLUMN.
SELECT add_attributes_column_to_link_table(format('%I.%I', table_schema, table_name) :: REGCLASS)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'link_table_%'
ORDER BY table_name;

DROP FUNCTION add_attributes_column_to_link_table(REGCLASS);
