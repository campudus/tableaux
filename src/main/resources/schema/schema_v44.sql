-- The backlink column of a link relation looks its links up by id_2, which the primary key (id_1, id_2) can't serve,
-- so reading its values scanned the whole link table once per row.
-- New link tables get the index via the CREATE TABLE DDL in ColumnModel.createLinkColumn.
-- Existing link tables are backfilled here.
CREATE OR REPLACE FUNCTION add_id_2_index_to_link_table(schema_name TEXT, link_table_name TEXT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I.%I (id_2)',
    'idx_' || link_table_name || '_id_2',
    schema_name,
    link_table_name
  );
  RETURN link_table_name;
END
$$ LANGUAGE plpgsql;

-- Same selection of link tables as schema_v43.
SELECT add_id_2_index_to_link_table(table_schema, table_name)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name LIKE 'link_table_%'
ORDER BY table_name;

DROP FUNCTION add_id_2_index_to_link_table(TEXT, TEXT);
