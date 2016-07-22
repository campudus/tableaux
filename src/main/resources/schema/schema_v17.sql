CREATE TABLE system_tablegroup (
  id BIGSERIAL,

  PRIMARY KEY (id)
);

CREATE TABLE system_tablegroup_lang (
  id          BIGINT      NOT NULL,
  langtag     VARCHAR(50) NOT NULL,
  name        VARCHAR(255),
  description TEXT,

  PRIMARY KEY (id, langtag),
  FOREIGN KEY (id) REFERENCES system_tablegroup (id) ON DELETE CASCADE
);

ALTER TABLE system_table
  ADD COLUMN group_id BIGINT,
  ADD FOREIGN KEY (group_id) REFERENCES system_tablegroup (id) ON DELETE SET NULL ON UPDATE CASCADE;