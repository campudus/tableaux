	create or replace function get_links_to_rowid(table_id integer) returns table(id_1 int8, id_2 int8) as $$
	begin
		return query execute 'select id_1, id_2 from link_table_' || table_id;
	end;
$$ language plpgsql;

select 
	l.link_id,
	l.table_id_1,
	l.table_id_2,
	c.identifier,
	c.column_id,
	lt.id_1,
	lt.id_2
from 
	system_link_table l
	left join system_columns c on (l.link_id = c.link_id)
	left join (select id_1, id_2 from get_links_to_rowid(24)) as lt on (lt.id_2 = 1 and l.link_id = 24)
where (table_id_2 = 2)
