#!/bin/bash
influx=influx-theodorc-eivinkop-s
docker run -d \
    -p 127.0.0.1:8086:8086 \
    -v $HOME/influx/data:/var/lib/influxdb2 \
    -v $HOME/influx/config:/etc/influxdb2 \
    --name $influx \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_USERNAME=root \
    -e DOCKER_INFLUXDB_INIT_PASSWORD=$INFLUX_PASS \
    -e DOCKER_INFLUXDB_INIT_BUCKET=pageviews_b \
    -e DOCKER_INFLUXDB_INIT_ORG=trcek \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=$INFLUX_TOKEN \
    influxdb:2.1.1
