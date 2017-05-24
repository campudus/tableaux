ALTER TABLE system_columns
  ADD COLUMN group_table_id BIGINT,
  ADD COLUMN group_column_id BIGINT;

ALTER TABLE system_columns
  ADD CONSTRAINT system_columns_group_fkey FOREIGN KEY (group_table_id, group_column_id) REFERENCES system_columns (table_id, column_id) ON DELETE SET NULL;