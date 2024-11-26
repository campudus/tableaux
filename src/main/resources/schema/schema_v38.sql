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

INSERT INTO system_annotations (name, priority, fg_color, bg_color, display_name, is_multilang, is_dashboard, is_custom)
	VALUES ('important', 1, '#ffffff', '#ff7474', '{"de":"Wichtig","en":"Important"}', FALSE, TRUE, FALSE);

INSERT INTO system_annotations (name, priority, fg_color, bg_color, display_name, is_multilang, is_dashboard, is_custom)
	VALUES ('check-me', 2, '#ffffff', '#c274ff', '{"de":"Bitte überprüfen","en":"Please double-check"}', FALSE, TRUE, FALSE);

INSERT INTO system_annotations (name, priority, fg_color, bg_color, display_name, is_multilang, is_dashboard, is_custom)
	VALUES ('postpone', 3, '#ffffff', '#999999', '{"de":"Später","en":"Later"}', FALSE, TRUE, FALSE);

INSERT INTO system_annotations (name, priority, fg_color, bg_color, display_name, is_multilang, is_dashboard, is_custom)
	VALUES ('needs_translation', 4, '#ffffff', '#ffae74', '{"de":"Übersetzung nötig","en":"Translation necessary"}', TRUE, TRUE, FALSE);