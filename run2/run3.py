import os.path
import csv
from neo4j_handler import Neo4j_Handle
import run_query
import time
graph_handler = Neo4j_Handle("http://localhost:7474", "neo4j", "changgong")
graph = graph_handler.graph

base_path = "../data/csv2"


class RUN:
    def __init__(self, index1, index2):
        self.index1 = index1
        self.index2 = index2

        run_query.clear()
        print("*"*50)
        # start_time = time.time()
        # self.run()
        # end_time = time.time()
        # print("INDEX: ", index1)
        # print(f"Query executed successfully in {end_time - start_time:.2f} seconds.")
        #
        # run_query.search()
        # run_query.update()

    def collect_time(self):
        collect_lis = []
        start_time = time.time()
        self.run()
        end_time = time.time()
        print("*" * 50)
        print("INDEX: ", self.index1)
        print(f"Query executed successfully in {end_time - start_time:.2f} seconds.")
        collect_lis.append(end_time - start_time)

        start_time = time.time()
        run_query.search()
        end_time = time.time()
        collect_lis.append(end_time - start_time)

        start_time = time.time()
        run_query.update()
        end_time = time.time()
        collect_lis.append(end_time - start_time)
        return collect_lis

    def run(self):
        fname_lis = os.listdir(base_path)
        for fname in fname_lis:
            try:
                self.create_node(fname)
            except Exception:
                pass

        self.create_relate("item.csv", "store_sales_item", "i_item_sk", ['store_sales', 'ss_item_sk'], ['item', 'i_item_sk'])
        self.create_relate("customer_address.csv", "store_sales_customer_address", "ca_address_sk", ['store_sales', 'ss_addr_sk'], ['item', 'ca_address_sk'])
        self.create_relate("store.csv", "store_sales_store", "s_store_sk", ['store_sales', 'ss_store_sk'], ['store', 's_store_sk'])
        self.create_relate("date_dim.csv", "store_sales_date_dim", "d_date_sk", ['store_sales', 'ss_sold_date_sk'], ['date_dim', 'd_date_sk'])
        self.create_relate("date_dim.csv", "store_dim", "d_date_sk", ['store', 'ss_sold_date_sk'], ['date_dim', 'd_date_sk'])

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

    def create_relate(self, fname, rel_name, key, left_lis=[], right_lis = []):
        left_name, left_key = left_lis
        right_name, right_key = right_lis

        path= os.path.join(base_path, fname)



        header = []
        try:
            with open(path, mode='r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                index = 0
                for row in csv_reader:
                    index += 1
                    if index > self.index2:
                        break
                    if not header:
                        header = row
                        continue
                    tmp_params = ""
                    for index, item in enumerate(header):
                        tmp_params += f"{item}: ${item}"
                        if index < len(header) - 1:
                            tmp_params += ", "
                    sql_str = "{ " + tmp_params + " }"
                    item_data = dict(zip(header, row))

                    query = (
                        f"MATCH (a:{left_name}), (b:{right_name}) "
                        f"WHERE a.{left_key} = $left_value AND b.{right_key} = $right_value "
                        f"MERGE(a)-[r:{rel_name} {sql_str}]->(b) "
                        "RETURN type(r) AS r"
                    )

                    tmp_value = item_data[key]
                    graph.run(query, left_value=tmp_value, right_value=tmp_value, **item_data)
        except Exception:
            pass

if __name__ == "__main__":
    RUN(100, 100).run()
    RUN(300, 300).run()
    RUN(500, 500).run()
