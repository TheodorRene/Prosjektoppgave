import ast

class CypherQueryGenerator:

    @staticmethod
    def insert_relation(from_node, to_node, node_type, id_field, properties, relation_name):
        """Create a Cypher query insertion string."""
        return f""" 
                MATCH
                (a:{node_type}),
                (b:{node_type})
                WHERE a.{id_field} = '{from_node}' AND b.{id_field} = '{to_node}'
                CREATE (a)-[:{relation_name} {properties if properties else ""}]->(b);
               """

class RevisionCypherQueryGenerator(CypherQueryGenerator):
    @staticmethod
    def insert_revision(csv_line):
        """TODO: Do something with the insert statement, e.g. store the text in a file"""
        title, timestamp, revisions = csv_line
        revisions = ast.literal_eval(revisions) # Evaluate revisions as a list and not as a string
        
        for revision in revisions:
            insert_statement = RevisionCypherQueryGenerator.insert_relation(from_node=title, to_node=revision, node_type="Page", id_field="id", properties=f"{{timestamp: '{timestamp}'}}", relation_name="LINKS_TO")
            print(insert_statement)