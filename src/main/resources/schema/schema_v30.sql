ALTER TABLE system_columns
  ADD COLUMN rules json NOT NULL DEFAULT '[]';
