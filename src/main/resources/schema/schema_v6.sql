ALTER TABLE system_link_table
DROP COLUMN column_id_1,
DROP COLUMN column_id_2,
ADD FOREIGN KEY (table_id_1) REFERENCES system_table (table_id) ON DELETE CASCADE,
ADD FOREIGN KEY (table_id_2) REFERENCES system_table (table_id) ON DELETE CASCADE;