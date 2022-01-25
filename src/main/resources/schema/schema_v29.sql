ALTER TABLE system_table
  ADD COLUMN attributes json NOT NULL DEFAULT '{}';
ALTER TABLE system_columns
  ADD COLUMN attributes json NOT NULL DEFAULT '{}';
