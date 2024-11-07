import os.path
import csv
from neo4j_handler import Neo4j_Handle
import run_query
import run_sql
import time

graph_handler = Neo4j_Handle("http://localhost:7474", "neo4j", "changgong")
graph = graph_handler.graph


base_path = "../data/csv2"

class RUN:
    def __init__(self, index1, index2):
        self.index1 = index1

        run_query.clear()


    def collect_time(self):
        collect_lis = []
        start_time = time.time()
        self.run()
        run_sql.run()
        end_time = time.time()
        print("*" * 50)
        print("INDEX: ", self.index1)
        print(f"Query executed successfully in {end_time - start_time:.2f} seconds.")
        collect_lis.append(end_time - start_time)

        start_time = time.time()
        run_query.search()
        run_sql.search()
        end_time = time.time()
        collect_lis.append(end_time - start_time)

        start_time = time.time()
        run_query.update()
        run_query.update()
        end_time = time.time()
        collect_lis.append(end_time - start_time)
        return collect_lis

    def run(self,):
        fname_lis = os.listdir(base_path)
        for fname in fname_lis:
            try:
                self.create_node(fname)
            except Exception:
                pass

    def create_node(self, fname: str):
        path_store_sales = os.path.join(base_path, fname)

        header = []
        sql_str = ""
        name = os.path.basename(path_store_sales).split(".")[0]
        with open(path_store_sales, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            index = 0
            for row in csv_reader:
                index += 1
                if index > self.index1:
                    break
                if not header:
                    tmp_params = ""
                    header = row
                    for index, item in enumerate(header):
                        tmp_params += f"{item}: ${item}"
                        if index < len(header) - 1:
                            tmp_params += ", "
                    sql_str = f"MERGE(n: {name} " + "{ "+ tmp_params +" }) RETURN n"
                    continue

                graph.run(sql_str, **dict(zip(header, row)))



if __name__ == "__main__":
    RUN(10).run()

