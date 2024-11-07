import os

import pymysql
import csv

 
# Database connection information

host = 'localhost'
user = 'root'
password = 'xjj20010113'
database = 'task_db_04'
sql_file_paths = [

]

base_path = "../data/csv2"



def run():
    try:
        connection = pymysql.connect(host=host,
                             user=user,
                             password=password
                             )
        with connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {database};")

            #  Creating a database

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
            print(f"Database '{database}' created or already exists.")

            #  Select database

            cursor.execute(f"USE {database};")

            for index, sql_file_path in enumerate(sql_file_paths):
                #  Reading SQL files
                try:
                    #  Executing SQL Scripts
                    continue
                    if index == 0:
                        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
                            sql_script = sql_file.read()

                        for statement in sql_script.split(';'):
                            statement = statement.strip()
                            if statement:  #  Make sure the statement is not empty
                                cursor.execute(statement)
                                print(f"Executed: {statement}")

                except Exception as e:
                    print("E: ", e)
                    connection.commit()
            for fpath in os.listdir(base_path):
                table_name = fpath.split('.')[0]
                # if table_name != "dbgen_version":
                #     continue
                path_store_sales = os.path.join(base_path, fpath)
                print("path_store_sales: ", path_store_sales)
                try:
                    with open(path_store_sales, mode='r', encoding='utf-8') as file:
                        csv_reader = csv.reader(file)
                        header = []
                        for index, row in enumerate(csv_reader):
                            if index == 0:
                                header = [f"`{item}`" for item in row]

                                table_sql = ""
                                for hdIndex, hd in enumerate(row):
                                    table_sql += f"{hd} VARCHAR(255) default ''"
                                    if hdIndex < len(row) - 1:
                                        table_sql += ", "
                                table_sql = f"CREATE TABLE {table_name} ( {table_sql} );"
                                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                                cursor.execute(table_sql)
                                continue

                            columns = ', '.join(header)
                            # get a value
                            values = []
                            # Processing the value of each line
                            row_values = ', '.join(f"'{str(value)}'" for value in row[:len(header)])
                            values.append(f"({row_values})")
                            values_str = ', '.join(values)
                            # insert_query = f"INSERT INTO {table_name} ({columns}) VALUES {values_str};"
                            insert_query = f"INSERT INTO {table_name}  VALUES {values_str};"
                            cursor.execute(insert_query)
                except Exception as e:
                    continue

        #  Submission of transactions
        connection.commit()
        print("All statements executed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  #  Rolling back transactions

    finally:
        connection.close()  #  Close connection
        print("Connection closed.")

def search():
    table = "lineitem"
    try:
        connection = pymysql.connect(host=host,
                             user=user,
                             password=password
                             )

        with connection.cursor() as cursor:
            cursor.execute(f"USE {database};")

            query = f"SELECT * FROM {table};"
            cursor.execute(query)
            results = cursor.fetchall()  #  Get all results
            print("Data in the table:")
            # for row in results:
            #     print(row)
    except Exception:
        pass

def update():
    table = "lineitem"
    try:
        connection = pymysql.connect(host=host,
                             user=user,
                             password=password
                             )
        with connection.cursor() as cursor:
            cursor.execute(f"USE {database};")

            update_query = f"UPDATE {table} SET L_TAX = 0.3;"
            cursor.execute(update_query)
            connection.commit()
    except Exception:
        pass

if __name__ == "__main__":
    run()
    # search()
    # update()