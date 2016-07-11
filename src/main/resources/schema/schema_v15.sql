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

CREATE OR REPLACE FUNCTION create_user_table_flag(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'CREATE TABLE user_table_flag_' || tableid || ' (
    row_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    uuid UUID NOT NULL,
    langtag VARCHAR(255) NOT NULL DEFAULT ''neutral'',
    type VARCHAR(255) NOT NULL,
    value TEXT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),

    PRIMARY KEY (row_id, column_id, uuid),
    FOREIGN KEY (row_id) REFERENCES user_table_' || tableid || ' (id) ON DELETE CASCADE
  )';

  RETURN 'user_table_flag_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT create_user_table_flag(table_id)
FROM system_table;

DROP FUNCTION create_user_table_flag( BIGINT );