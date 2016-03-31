ALTER TABLE system_table
ADD COLUMN
ordering BIGINT;

UPDATE system_table
SET ordering = table_id;

ALTER TABLE system_table
ALTER COLUMN ordering
SET DEFAULT currval('system_table_table_id_seq');