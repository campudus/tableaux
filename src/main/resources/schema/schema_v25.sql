CREATE OR REPLACE FUNCTION create_history_tables(tableid BIGINT)
RETURNS TEXT AS $$
BEGIN

EXECUTE 'CREATE TABLE user_table_history_' || tableid || '(
  revision BIGSERIAL,
  row_id BIGINT NOT NULL,
  column_id BIGINT,
  event VARCHAR(255) NOT NULL DEFAULT ''cell_changed'',
  type VARCHAR(255),
  language_type VARCHAR(255) DEFAULT ''neutral'',
  author VARCHAR(255),
  timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
  value JSON NULL,
  PRIMARY KEY (revision)
)';
RETURN 'user_table_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT create_history_tables(table_id)
FROM system_table;

DROP FUNCTION create_history_tables( BIGINT );
