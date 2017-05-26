CREATE TABLE system_column_groups (
  table_id          BIGINT,
  group_column_id   BIGINT,
  grouped_column_id BIGINT,
  PRIMARY KEY (table_id, group_column_id, grouped_column_id),
  FOREIGN KEY (table_id, group_column_id) REFERENCES system_columns (table_id, column_id) ON DELETE CASCADE,
  FOREIGN KEY (table_id, grouped_column_id) REFERENCES system_columns (table_id, column_id) ON DELETE CASCADE
);