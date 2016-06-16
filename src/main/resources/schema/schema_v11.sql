CREATE OR REPLACE FUNCTION convertMultilanguageToString(multilanguage BOOLEAN)
  RETURNS VARCHAR AS
$BODY$
SELECT CASE WHEN $1
  THEN 'language'
       ELSE NULL END;
$BODY$
LANGUAGE 'sql' IMMUTABLE STRICT;

ALTER TABLE system_columns
  ALTER COLUMN multilanguage TYPE VARCHAR(255) USING convertMultilanguageToString(multilanguage);

DROP FUNCTION convertMultilanguageToString( BOOLEAN );

ALTER TABLE system_columns ADD COLUMN country_codes TEXT ARRAY;