CREATE TABLE system_columns_lang (
  table_id    BIGINT      NOT NULL,
  column_id   BIGINT      NOT NULL,
  langtag     VARCHAR(50) NOT NULL,
  name        VARCHAR(255),
  description TEXT,

  PRIMARY KEY (table_id, column_id, langtag),
  FOREIGN KEY (table_id, column_id) REFERENCES system_columns (table_id, column_id) ON DELETE CASCADE
);