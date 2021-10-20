#!/bin/bash
docker run -p [::1]:7474:7474 -p [::1]:7687:7687 -d -e NEO4J_AUTH=neo4j/$NEO4J_PASS neo4j:4.3.6
