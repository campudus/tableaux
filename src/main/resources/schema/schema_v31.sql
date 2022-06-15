do
$$
declare
	row record;
begin
	for row in select table_name from information_schema.tables where table_name like 'link_table_%'
	loop 
		execute 'ALTER TABLE public.' || quote_ident(row.table_name) || ' ADD COLUMN links_from jsonb DEFAULT NULL;';
	end loop;
end;
$$;	
