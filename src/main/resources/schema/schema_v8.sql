CREATE UNIQUE INDEX system_columns_name ON system_columns USING BTREE (table_id, user_column_name);

CREATE OR REPLACE FUNCTION drop_unnecessary_sequence(seqname REGCLASS)
  RETURNS TEXT AS $$
BEGIN
  EXECUTE 'DROP SEQUENCE IF EXISTS ' || seqname || ' CASCADE';
  RETURN seqname :: TEXT;
END
$$ LANGUAGE plpgsql;

SELECT drop_unnecessary_sequence(sequence_name :: REGCLASS)
FROM information_schema.sequences
WHERE sequence_name LIKE 'user_table_lang_%_id_seq';

DROP FUNCTION drop_unnecessary_sequence( REGCLASS );