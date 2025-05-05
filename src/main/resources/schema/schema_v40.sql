CREATE OR REPLACE FUNCTION add_deleted_at_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_history_' || tableid || ' ADD COLUMN deleted_at timestamp without time zone';
  RETURN 'user_table_history_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_deleted_at_column(table_id)
FROM system_table;

DROP FUNCTION add_deleted_at_column( BIGINT );

