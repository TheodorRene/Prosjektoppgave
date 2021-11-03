#!/bin/bash
query=$(cat << EOF
CREATE TABLE pagelinks_complete AS(
 SELECT
   p.page_id as from_page_id, p.page_title as from_page_title,
   p2.page_id as to_page_id, p2.page_title as to_page_title
 FROM
  pagelinks pl
 INNER JOIN
  page p
    ON
      pl.pl_from=p.page_id
 INNER JOIN
   page p2
   ON
     pl.pl_title=p2.page_title
 WHERE
   pl.pl_namespace=0 AND pl.pl_from_namespace=0)
EOF
)
echo $query > make_pagelinks.sql
mdb=mariadb-theodorc-eivinkop-s
echo -n "inserting data " && date
docker exec -i $mdb sh -c 'exec mysql -uroot -p"$MARIADB_ROOT_PASSWORD" pagelinks-db' < make_pagelinks.sql
echo -n "Finito " && date
