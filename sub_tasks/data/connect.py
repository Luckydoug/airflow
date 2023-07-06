import psycopg2
import pandas as pd
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.exceptions import AirflowException
#import pygrametl
#from pygrametl.tables import SlowlyChangingDimension
import io
import datetime;
import mysql.connector as database
from sqlalchemy.engine import create_engine

LOCAL_DIR = "/tmp/"
engine = create_engine('postgresql://postgres:@Akb@rp@$$w0rtf31n@10.40.16.19:5432/mabawa')
#engineString = 'mariadb+mariadbconnector://optica-admin:%s@10.40.16.17/mwangaza' % urlquote('@MWenendo../')
#mariaengine = create_engine(engineString)
username = "optica-admin"
password = "@MWenendo../"

connection = database.connect(
    user=username,
    password=password,
    host="10.40.16.17",
    database="mwangaza")

"""
    Slow Changing dimention ETL to be used for any dimention table
"""
"""
def etl_slowchangingdim(targ_table, targ_key, targ_lookup_column, tag_attributes, query):
    try:
        db_conn = psycopg2.connect(user="postgres",
                                                password="@MAdrassa/..",
                                                # host="197.232.20.203",
                                                host="10.40.16.19",
                                                port="5432",
                                                database="mabawa")


        connection = pygrametl.ConnectionWrapper(db_conn)
        connection.setasdefault()

        dimension_table = SlowlyChangingDimension(
            name=targ_table,
            key=targ_key,
            attributes=tag_attributes,
            lookupatts=targ_lookup_column,
            fromatt="date_from",
            toatt="date_to",
            versionatt="version", 
            cachesize=-1)

        for row in query:
            dimension_table.scdensure(row)
        connection.commit()


    except (Exception, psycopg2.Error) as error :
        print ("Error while inserting to slow changing dimension", error)
        raise AirflowException("etl_slowchangingdim Error")
    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("etl_slowchangingdim connection is closed") 
"""   

"""
    Postgres Function to connect and select data based on the provided query
"""

def pg_fetch_all(query):
    try:
        connection = psycopg2.connect(user="postgres",
                                        password="@Akb@rp@$$w0rtf31n",
                                        host="10.40.16.19",
                                        port="5432",
                                        database="mabawa")

        fetched_data = pd.read_sql_query(query, connection)


        # print(type(fetched_data))
        return fetched_data

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise AirflowException("pg_fetch_all Error")

    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("pg_fetch_all connection is closed")
"""
    Postgres Function to connect and select data based on the provided query 
    and create a csv file  
"""
def pg_fetch_all_write(query, file_name):
    try:
        connection = psycopg2.connect(user="postgres",
                                        password="@Akb@rp@$$w0rtf31n",
                                        host="10.40.16.19",
                                        port="5432",
                                        database="mabawa")

        fetched_data = pd.read_sql_query(query, connection)


        # print(type(fetched_data))
        # write file to csv
        fetched_data.to_csv(file_name, index=False)
        return fetched_data

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise AirflowException("pg_fetch_all Error")

    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("pg_fetch_all connection is closed")


"""
    Postgres Function to connect and execute a query based on the provided query
"""

def pg_execute(query):
    try:
        connection = psycopg2.connect(user="postgres",
                                        password="@Akb@rp@$$w0rtf31n",
                                        host="10.40.16.19",
                                        port="5432",
                                        database="mabawa")

        #fetched_data = pd.read_sql_query(query, connection)

        curr = connection.cursor()
        curr.execute(query)
        count = curr.rowcount
        # print(type(fetched_data))
        connection.commit()
        connection.close()
        print(count, " executed queries")
        return count

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
        raise AirflowException("mssql_fetch_all Error")
    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("pg_truncate connection is closed")       


"""
    Pandas insert into a table
"""

def pg_insert_dataframe(pd_result, table, schema_name):
    engine = create_engine( 'postgresql+psycopg2://postgres:@Akb@rp@$$w0rtf31n@10.40.16.19/mabawa')


    #fetched_data = pd.read_sql_query(query, connection)

    pd_result.to_sql(table, con = engine, if_exists = 'append', chunksize = 1000, index=False, schema=schema_name)

def pg_bulk_insert(pd_result, table_name, schema):
    engine = create_engine( 'postgresql+psycopg2://postgres:@Akb@rp@$$w0rtf31n@10.40.16.19/mabawa')

    ct = datetime.datetime.now() 
    print("current time:-", ct)     

    string_data_io = io.StringIO()
    pd_result.to_csv(string_data_io, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(engine, schema=schema)
    table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=pd_result,
                                index=False, if_exists='append', schema=schema)
    table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY %s.%s FROM STDIN HEADER DELIMITER '|' CSV" % (schema, table_name)
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()

    ct = datetime.datetime.now() 
    print("current time:-", ct)