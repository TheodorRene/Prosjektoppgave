import ast

class CypherQueryGenerator:

    @staticmethod
    def insert_relation(from_node, to_node, node_type, id_field, properties, relation_name):
        """Create a Cypher query insertion string."""
        return "" + \
                ("MATCH "
                f"(a:{node_type}),"
                f"(b:{node_type}) "
                f"WHERE a.{id_field} = {from_node} AND b.{id_field} = {to_node} "
                f"CREATE (a)-[:{relation_name} {properties if properties else ''}]->(b);")

class RevisionCypherQueryGenerator(CypherQueryGenerator):
    @staticmethod
    def insert_revision(csv_line):
        title, timestamp, revisions = csv_line
        revisions = ast.literal_eval(revisions) # Evaluate revisions as a list and not as a string
        
        for revision in revisions:
            insert_statement = RevisionCypherQueryGenerator.insert_relation(from_node=title, to_node=revision, node_type="Page", id_field="id", properties=f"{{timestamp: '{timestamp}'}}", relation_name="LINKS_TO")
            print(insert_statement)

    @staticmethod
    def insert_revision_with_time_interval(csv_line):
        from_id, to_id, timestamps = csv_line
        from_timestamp, to_timestamp = timestamps.split(",")
        from_timestamp.replace('"', '')
        to_timestamp.replace('"', '')

        if to_timestamp == "None":
            to_timestamp_insertion = "null"
        else:
            to_timestamp_insertion = f"'{to_timestamp}'"
        from_timestamp_insertion = f"'{from_timestamp}'"
        insert_statement = CypherQueryGenerator.insert_relation(from_node=from_id, to_node=to_id, node_type="Page", id_field="id", properties=f"{{from_timestamp: {from_timestamp_insertion}, to_timestamp: {to_timestamp_insertion}}}", relation_name="LINKS_TO")
        return insert_statement