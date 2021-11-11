import os
config = {
        "debug":False,
        "uri":"bolt://localhost:7687",
        "user": "neo4j",
        "password":os.environ["NEO4J_PASS"],
        "dry_run":True,
        "only_norway":True,
        "only_pages":True,
        "only_include_desktop":True
        }

pagelinks_config = {
    "debug":False,
    "host":"localhost",
    "user":"root",
    "password":os.environ["MARIADB_ROOT_PASSWORD"],
    "database":"pagelinks-db",
    "dry_run":True,
    "use_pagelinks_complete":True
}

influx_config = {
    "debug":False,
    "dry_run":True,
    "password":os.environ["INFLUX_PASS"],
    "user": "root",
    "org":"trcek",
    "token":os.environ["INFLUX_TOKEN"],
    "url":"http://localhost:8086",
    "query":True
}
