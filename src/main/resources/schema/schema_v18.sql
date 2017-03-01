CREATE OR REPLACE FUNCTION drop_needs_translation_column_and_change_to_langtags(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_flag_' || tableid || ' ADD COLUMN langtags TEXT[] DEFAULT NULL';
  EXECUTE 'ALTER TABLE user_table_flag_' || tableid || ' DROP COLUMN langtag';

  EXECUTE 'ALTER TABLE user_table_flag_' || tableid || ' RENAME TO user_table_annotations_' || tableid || '';

  EXECUTE 'ALTER TABLE user_table_lang_' || tableid || ' DROP COLUMN needs_translation';

  RETURN 'user_table_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT drop_needs_translation_column_and_change_to_langtags(table_id)
FROM system_table;

DROP FUNCTION drop_needs_translation_column_and_change_to_langtags( BIGINT );