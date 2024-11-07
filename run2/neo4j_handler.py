from py2neo import Graph,Node,Relationship,NodeMatcher, Subgraph
from py2neo.bulk import create_nodes, merge_nodes, create_relationships, merge_relationships

import time
class Neo4j_Handle():
    graph = None
    def __init__(self, url, name, password):
        while True:
            try:
                self.graph = Graph(url, name="neo4j", auth=(name, password))
                print("Neo4j Success ...")
                break
            except Exception as e:
                print("Neo4j Error ...")
                time.sleep(5)


    def clear_db(self):
        sql = "MATCH (n) DETACH DELETE n"
        self.graph.run(sql)

    def matchEntityItem(self):
        sql = "MATCH (entity1)  RETURN entity1"
        print(sql)
        answer = self.graph.run(sql).data()
        return answer

    def batch_create(self, nodes_list, relations_list):


        subgraph = Subgraph(nodes_list, relations_list)
        tx_ = self.graph.begin()
        tx_.create(subgraph)
        # tx_.merge(subgraph)
        self.graph.commit(tx_)

