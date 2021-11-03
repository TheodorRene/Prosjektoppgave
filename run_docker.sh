#!/bin/bash
neo=neo4j-theodorc-eivinkop-s
docker run -d \
    -p 127.0.0.1:7474:7474 \
    -p 127.0.0.1:7687:7687 \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    --name $neo \
    -e NEO4J_AUTH=neo4j/$NEO_PASS \
    -e NEO4JLABS_PLUGINS='["apoc", "graph-data-science"]' \
    -e NEO4J_dbms_security_procedures_unrestricted='apoc.*,gds.*' \
    -e dbms.security.procedures.allowlist='apoc.*,gds.*' \
    neo4j:4.3.6 
