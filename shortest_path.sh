#!/bin/bash
if [ $# -eq 0 ]; then
    echo 'match (n:Page{title:"ARG1"}, (q:Page{title:"ARG2"}), p =shortestPath((n)-[*]->(q)) return p;'
    echo "Returns shortest path between argument #1 and argument #2"
    exit 0
fi
arg1=$1
arg2=$2
str='match (n:Page{title:"'
str2='"}), (q:Page{title:"'
str3='"}), p = shortestPath((n) -[*]-> (q)) return p;'
echo "$str$arg1$str2$arg2$str3" | cypher-shell -u neo4j -p $NEO_PASS
