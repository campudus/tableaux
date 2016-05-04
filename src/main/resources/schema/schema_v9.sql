ALTER TABLE system_table
  ADD COLUMN langtags TEXT ARRAY;

CREATE TABLE system_settings (
  key   VARCHAR(255) PRIMARY KEY,
  value TEXT
);

INSERT INTO system_settings (key, value) VALUES ('langtags', $$["de-DE", "en-GB"]$$);