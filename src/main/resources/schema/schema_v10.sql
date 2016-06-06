CREATE OR REPLACE FUNCTION create_ordering_for_link_table(link_table REGCLASS)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'ALTER TABLE ' || link_table || ' ADD COLUMN ordering_1 SERIAL, ADD COLUMN ordering_2 SERIAL';
  EXECUTE 'UPDATE ' || link_table || ' SET ordering_1 = id_1, ordering_2 = id_2';

  RETURN link_table :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT create_ordering_for_link_table(table_name :: REGCLASS)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'link_table_%'
ORDER BY table_name;

DROP FUNCTION create_ordering_for_link_table( REGCLASS );

CREATE TABLE system_table_lang (
  table_id    BIGINT      NOT NULL,
  langtag     VARCHAR(50) NOT NULL,
  name        VARCHAR(255),
  description TEXT,

  PRIMARY KEY (table_id, langtag),
  FOREIGN KEY (table_id) REFERENCES system_table (table_id) ON DELETE CASCADE
);

INSERT INTO system_table_lang (table_id, langtag, name, description)
  SELECT table_id, (SELECT value::json->>0 from system_settings where key = 'langtags'), user_table_name, null FROM system_table;