#!/bin/bash
set -e
which docker
which mycli
echo "INITALIZING MARIADB"
docker run -p3306:3306 \
    --name pagelinks-theodorc-eivinkop \
    -e MARIADB_ROOT_PASSWORD=my-secret-pw \
    -e MARIADB_DATABASE=pagelinks-db \
    mariadb:10.4.19 \
echo "Inserting data, this will take a looong time"
mycli 
    -h 127.0.0.1 \
    -u root \
    --password=my-secret-pwd \
    pagelinks-db < $1



    
