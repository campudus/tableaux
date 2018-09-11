ALTER TABLE system_columns_lang
  ADD FOREIGN KEY (table_id) REFERENCES system_table (table_id) ON DELETE CASCADE;

ALTER TABLE system_column_groups
  ADD FOREIGN KEY (table_id) REFERENCES system_table (table_id) ON DELETE CASCADE;

ALTER TABLE system_attachment
  ADD FOREIGN KEY (table_id) REFERENCES system_table (table_id) ON DELETE CASCADE;


CREATE OR REPLACE FUNCTION drop_needs_translation_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_lang_' || tableid || ' DROP COLUMN IF EXISTS needs_translation';
  RETURN 'user_table_lang_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT drop_needs_translation_column(table_id)
FROM system_table;

DROP FUNCTION drop_needs_translation_column( BIGINT );
