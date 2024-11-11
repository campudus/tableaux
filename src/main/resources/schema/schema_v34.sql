CREATE OR REPLACE FUNCTION update_user_tables(tableid BIGINT)
RETURNS TEXT AS $$
BEGIN

EXECUTE 'ALTER TABLE public.user_table_' || tableid || ' ADD COLUMN row_permissions jsonb DEFAULT NULL;';
RETURN 'user_table_' || tableid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT update_user_tables(table_id)
FROM system_table;

DROP FUNCTION update_user_tables( BIGINT );
