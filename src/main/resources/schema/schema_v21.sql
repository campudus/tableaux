CREATE OR REPLACE FUNCTION remove_dead_annotations(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN

  EXECUTE 'DELETE FROM user_table_annotations_' || tableid || ' u WHERE NOT EXISTS (SELECT 1 FROM system_columns c WHERE c.table_id = ' || tableid || ' AND u.column_id = c.column_id)';

  RETURN 'user_table_' || tableid :: TEXT;

END
$$
LANGUAGE plpgsql;

SELECT remove_dead_annotations(table_id)
FROM system_table;

DROP FUNCTION remove_dead_annotations( BIGINT );