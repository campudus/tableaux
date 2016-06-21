CREATE SEQUENCE update_unique_folders;
UPDATE
  folder
SET
  name = folder.name || ' ' || nextval('update_unique_folders')
FROM
  (SELECT
     idparent,
     name,
     COUNT(*) AS count
   FROM folder
   GROUP BY idparent, name) AS sub
WHERE
  count > 1
  AND
  folder.idparent = sub.idparent
  AND
  folder.name = sub.name;
DROP SEQUENCE update_unique_folders;

CREATE UNIQUE INDEX system_folder_name ON folder (name, idparent);