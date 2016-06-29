ALTER TABLE system_table ADD COLUMN type TEXT;
UPDATE system_table SET type = 'generic';