CREATE OR REPLACE FUNCTION add_deleted_at_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_history_' || tableid || ' ADD COLUMN deleted_at timestamp without time zone';
  RETURN 'user_table_history_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_deleted_at_column(table_id) FROM system_table;
DROP FUNCTION add_deleted_at_column( BIGINT );


CREATE OR REPLACE FUNCTION change_history_currency_zero_values_to_null(tableid BIGINT)
  RETURNS TEXT AS $$
DECLARE
  tablename TEXT := 'user_table_history_' || tableid;
  query TEXT;

BEGIN
  query := FORMAT($f$
    UPDATE %I AS t
    SET value = jsonb_set(t.value::jsonb,
                          ARRAY['value', sub.key],
                          'null'::jsonb)
    FROM (
      SELECT revision, key
      FROM %I,
      LATERAL jsonb_each(value::jsonb -> 'value') AS v(key, val)
      WHERE value_type = 'currency' AND val = '0'
    ) AS sub
    WHERE t.revision = sub.revision
  $f$, tablename, tablename);

  EXECUTE query;
  RETURN tablename;
END
$$ LANGUAGE plpgsql;

SELECT change_history_currency_zero_values_to_null(table_id) FROM system_table;
DROP FUNCTION change_history_currency_zero_values_to_null( BIGINT );


CREATE OR REPLACE FUNCTION add_indexes_to_history_table(tableid BIGINT)
  RETURNS TEXT AS $$
DECLARE
  tablename TEXT := 'user_table_history_' || tableid;
  query TEXT;

BEGIN
  EXECUTE 'CREATE INDEX idx_user_table_history_' || tableid || '_row_id ON user_table_history_' || tableid || '(row_id)';
  EXECUTE 'CREATE INDEX idx_user_table_history_' || tableid || '_col_id ON user_table_history_' || tableid || '(column_id)';
  EXECUTE 'CREATE INDEX idx_user_table_history_' || tableid || '_row_col_id ON user_table_history_' || tableid || '(row_id, column_id)';

  RETURN 'user_table_history_' || tableid::TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_indexes_to_history_table(table_id) FROM system_table;
DROP FUNCTION add_indexes_to_history_table( BIGINT );

CREATE OR REPLACE FUNCTION migrate_json_to_jsonb(tableid BIGINT)
  RETURNS TEXT AS $$
DECLARE
  tablename TEXT := 'user_table_history_' || tableid;

BEGIN
  EXECUTE FORMAT($f$
    ALTER TABLE %I ALTER COLUMN value TYPE jsonb USING value::jsonb
  $f$, tablename);

  RETURN tablename;
END
$$ LANGUAGE plpgsql;

SELECT migrate_json_to_jsonb(table_id) FROM system_table;
DROP FUNCTION migrate_json_to_jsonb( BIGINT );
