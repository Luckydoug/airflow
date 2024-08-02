import sys
sys.path.append(".")
from sub_tasks.data.connect import pg_execute

def edit_source_tables():

    with open('/home/opticabi/airflow/dags/sub_tasks/postgres/source_table_edits.txt') as f:
        sql_queries = f.read()
    
    queries = sql_queries.split(';')

    for query in queries:
        if not query.strip():
            continue
        try:
            pg_execute(query)
        except Exception as e:
            print(f"Error executing query:\n{query}\n{e}")

# edit_source_tables()