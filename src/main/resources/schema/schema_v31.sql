CREATE OR REPLACE FUNCTION update_link_tables(linkid BIGINT)
RETURNS TEXT AS $$
BEGIN

EXECUTE 'ALTER TABLE public.link_table_' || linkid || ' ADD COLUMN links_from jsonb DEFAULT NULL;';
RETURN 'link_table_' || linkid :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT update_link_tables(link_id)
FROM system_link_table;
