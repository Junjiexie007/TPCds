import time
import pymysql
from neo4j_handler import Neo4j_Handle

graph_handler = Neo4j_Handle("http://localhost:7474", "neo4j", "changgong")
graph = graph_handler.graph

def run_query(query: str):
    """
    Execute time-consuming queries and handle exceptions.

    :param query: The Cypher query to execute.
    """
    try:
        start_time = time.time()
        result = graph.run(query)
        end_time = time.time()
        print(f"Query executed successfully in {end_time - start_time:.2f} seconds.")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")

def search():
    long_running_query = """
    MATCH p = (n) -[r:RELATIONSHIP_TYPE*0..5]- (b)
    RETURN p, COUNT(r) AS relationship_count
    """
    run_query(long_running_query)

def update():
    try:
        start_time = time.time()
        result = graph.run("""
            MATCH (n) 
            SET n.params = 'test_params'
        """)
        end_time = time.time()
        print(f"Query update successfully in {end_time - start_time:.2f} seconds.")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")

def clear():
    query = """
    CALL apoc.periodic.iterate(
  "MATCH (n) RETURN n", // Match the node to be deleted
  "DETACH DELETE n", // Delete the node and all related relationships
  {batchSize:1000, parallel:true}
)
    """
    try:
        query = "MATCH (n) DETACH DELETE n"
        graph.run(query)
        graph.run(query)
        graph.run(query)
        graph.run(query)
        graph.run(query)
    except Exception as e:
        print(f"Error executing query: {e}")