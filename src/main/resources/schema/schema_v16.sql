CREATE UNIQUE INDEX system_table_name
  ON system_table USING BTREE (user_table_name);