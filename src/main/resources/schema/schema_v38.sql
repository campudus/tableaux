CREATE TABLE system_annotations (
  name          VARCHAR(50) NOT NULL,
  priority      SERIAL,
  fg_color      VARCHAR(50) NULL,
  bg_color      VARCHAR(50) NULL,
  display_name  JSON,
  is_multilang  BOOLEAN NOT NULL DEFAULT FALSE,
  is_dashboard  BOOLEAN NOT NULL DEFAULT TRUE,
  is_custom     BOOLEAN NOT NULL DEFAULT TRUE,

  PRIMARY KEY (name)
);

INSERT INTO system_annotations
  (name, priority, fg_color, bg_color, display_name, is_multilang, is_dashboard, is_custom)
VALUES
  ('important',         1, '#ffffff', '#ff7474', '{"de":"Wichtig","en":"Important"}',                       FALSE, TRUE, TRUE),
  ('check-me',          2, '#ffffff', '#c274ff', '{"de":"Bitte überprüfen","en":"Please double-check"}',    FALSE, TRUE, TRUE),
  ('postpone',          3, '#ffffff', '#999999', '{"de":"Später","en":"Later"}',                            FALSE, TRUE, TRUE),
  ('needs_translation', 4, '#ffffff', '#ffae74', '{"de":"Übersetzung nötig","en":"Translation necessary"}', TRUE,  TRUE, FALSE);

CREATE OR REPLACE FUNCTION add_annotation_name_column(tableid BIGINT)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE user_table_annotations_' || tableid || ' ADD COLUMN annotation_name TEXT NULL';
  EXECUTE 'ALTER TABLE user_table_annotations_' || tableid || ' ADD FOREIGN KEY (annotation_name) REFERENCES system_annotations (name) ON DELETE CASCADE ON UPDATE CASCADE';
  EXECUTE 'UPDATE user_table_annotations_' || tableid || ' SET annotation_name = value WHERE type = ''flag''';
  EXECUTE 'UPDATE user_table_annotations_' || tableid || ' SET value = NULL WHERE type = ''flag''';
  RETURN 'user_table_annotations_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT add_annotation_name_column(table_id)
FROM system_table;

DROP FUNCTION add_annotation_name_column( BIGINT );