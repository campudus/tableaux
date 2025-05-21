CREATE TABLE system_union_table (
  table_id BIGINT REFERENCES system_table(table_id) ON DELETE CASCADE,
  origin_table_id BIGINT REFERENCES system_table(table_id) ON DELETE CASCADE,
  CONSTRAINT system_union_table_pkey PRIMARY KEY (table_id, origin_table_id)
);

CREATE TABLE system_union_column (
  table_id BIGINT REFERENCES system_table(table_id) ON DELETE CASCADE,
  column_id BIGINT, 
  origin_table_id BIGINT REFERENCES system_table(table_id) ON DELETE CASCADE,
  origin_column_id bigint,
  CONSTRAINT system_union_column_pkey PRIMARY KEY (table_id, column_id, origin_table_id, origin_column_id),
  CONSTRAINT fk_union_table_fkey FOREIGN KEY (table_id, origin_table_id) REFERENCES system_union_table (table_id, origin_table_id) ON DELETE CASCADE,
  CONSTRAINT fk_union_column_fkey FOREIGN KEY (table_id, column_id) REFERENCES system_columns (table_id, column_id) ON DELETE CASCADE,
  CONSTRAINT fk_union_origin_column_fkey FOREIGN KEY (origin_table_id, origin_column_id) REFERENCES system_columns (table_id, column_id) ON DELETE CASCADE
);
