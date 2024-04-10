CREATE OR REPLACE FUNCTION add_archived_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_' || tableid || ' ADD COLUMN archived BOOLEAN DEFAULT false';
  RETURN 'user_table_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_archived_column(table_id)
FROM system_table;

DROP FUNCTION add_archived_column( BIGINT );
