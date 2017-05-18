ALTER TABLE system_link_table
  ADD COLUMN cardinality_1 INTEGER DEFAULT '0',
  ADD COLUMN cardinality_2 INTEGER DEFAULT '0',
  ADD COLUMN delete_cascade BOOLEAN DEFAULT 'FALSE',
  ADD CHECK (cardinality_1 >= 0),
  ADD CHECK (cardinality_2 >= 0);