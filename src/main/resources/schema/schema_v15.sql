CREATE OR REPLACE FUNCTION add_final_and_translation_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_' || tableid || ' ADD COLUMN final BOOLEAN DEFAULT false';
  EXECUTE 'ALTER TABLE user_table_lang_' || tableid || ' ADD COLUMN needs_translation BOOLEAN DEFAULT false';
  RETURN 'user_table_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_final_and_translation_column(table_id)
FROM system_table;

DROP FUNCTION add_final_and_translation_column( BIGINT );