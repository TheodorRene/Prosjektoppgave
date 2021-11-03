#!/bin/bash
# 1. Download all relevant files
#   1.1.1 Download page data
#   1.1.2 Download pagelinks
#   1.1.3 Download pageviews
# 1.2 Start docker containers
#   1.2.1 Start mariadb
#   1.2.1 Start neo4j
# 2. Insert data in database
#   2.1 insert page data
#   2.2 insert pagelinks
# 3. Insert pagelinks_complete

echo -n "Starting script " && date

echo -n "Sourcing venv " && date
source env.sh

arg1=20210901
year=${arg1:0:4}
month=${arg1:4:2}
day=${arg1:6:2}

echo -n "Downloading dumps" && date
([ -f page_${arg1}.sql ] || (curl -s https://dumps.wikimedia.org/nowiki/${arg1}/nowiki-${arg1}-page.sql.gz -o page_${arg1}.sql.gz && gzip -d page_${arg1}.sql.gz)) &
P1=$!
([ -f pagelinks_${arg1}.sql ] || (curl -s https://dumps.wikimedia.org/nowiki/${arg1}/nowiki-${arg1}-pagelinks.sql.gz -o pagelinks_${arg1}.sql.gz && gzip -d pagelinks_${arg1}.sql.gz)) &
P2=$!
([ -f pageview_${arg1} ] || (curl -s https://dumps.wikimedia.org/other/pageview_complete/${year}/${year}-${month}/pageviews-${arg1}-user.bz2 -o pageview_${arg1}.bz2 && bzip2 -d pageview_${arg1}.bz2)) &
P3=$!


mdb=mariadb-theodorc-eivinkop-s
neo=neo4j-theodorc-eivinkop-s

echo -n "Deleting containers" && date
docker container rm -f $mdb
docker container rm -f $neo

echo -n "Starting containers" && date
docker run -d \
    -p 127.0.0.1:3306:3306 \
    --name $mdb \
    -v mariadb-vol:/var/lib/mysql \
    -e MARIADB_ROOT_PASSWORD=$MARIADB_ROOT_PASSWORD \
    -e MARIADB_DATABASE=pagelinks-db \
    mariadb:10.4.19 &
P4=$!
docker run -d \
    -p 127.0.0.1:7474:7474 \
    -p 127.0.0.1:7687:7687 \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    --name $neo \
     -e NEO4J_AUTH=neo4j/$NEO4J_PASS \
    neo4j:4.3.6 &
P5=$!

echo -n "waiting for $P1 $P2 $P3 $P4 $P5" && date
wait $P1 $P2 $P3 $P4 $P5
echo -n "INSERTING DATA" && date

echo "Sleeping to make sure the docker containers are ready"
sleep 4

echo "Inserting data"
docker exec -i $mdb sh -c 'exec mysql -uroot -p"$MARIADB_ROOT_PASSWORD" pagelinks-db' < page_${arg1}.sql &
P6=$!
echo "Inserting more data"
docker exec -i $mdb sh -c 'exec mysql -uroot -p"$MARIADB_ROOT_PASSWORD" pagelinks-db' < pagelinks_${arg1}.sql &
P7=$!
wait $P6 $P7
echo -n "Done with inserting page and pagelinks " && date
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

echo -n "inserting data " && date
docker exec -i $mdb sh -c 'exec mysql -uroot -p"$MARIADB_ROOT_PASSWORD" pagelinks-db' < make_pagelinks.sql
echo -n "Done with pagelinks_complete " && date
source ../venv/bin/activate && python ../pagelinks.py

echo -n "Finito " && date




