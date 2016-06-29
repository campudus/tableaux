CREATE OR REPLACE FUNCTION set_null_boolean_to_false(tablename TEXT, columnname TEXT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'UPDATE ' || tablename || ' SET ' || columnname || ' = FALSE WHERE ' || columnname || ' IS NULL;';
  EXECUTE 'ALTER TABLE ' || tablename || ' ALTER COLUMN ' || columnname || ' SET DEFAULT FALSE';

  RETURN tablename || ' ' || columnname :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT set_null_boolean_to_false(sub.tablename, sub.columnname)
FROM
  (
    SELECT
      CASE
      WHEN multilanguage IS NULL
        THEN 'user_table_' || table_id
      WHEN multilanguage IS NOT NULL
        THEN 'user_table_lang_' || table_id
      END                    AS tablename,
      'column_' || column_id AS columnname
    FROM system_columns
    WHERE column_type = 'boolean'
  ) AS sub;

DROP FUNCTION set_null_boolean_to_false( TEXT, TEXT );