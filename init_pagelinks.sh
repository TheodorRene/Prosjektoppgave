#!/bin/bash
set -e
which docker
which mysql
echo "INITALIZING MARIADB"
docker run -e3306:3306 \
    --name pagelinks-db \
    -e MARIADB_ROOT_PASSWORD=my-secret-pw \
    mariadb:10.4.19 \
echo "Inserting data, this will take a looong time"
mysql 
    -h 127.0.0.1 \
    -u root \
    --password=my-secret-pwd \
    pagelinks-db < $1



    
