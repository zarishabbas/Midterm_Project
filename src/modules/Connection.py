import psycopg2
from psycopg2 import Error
import pandas as pd
import numpy as np

def create_connection(host, port, user, pwd, database):

    '''
    Creates a connection to SQL postgres db.
    
    Accepts:
        host (str)
        port (int)
        user (str)
        pwd (str)
        database (str)
    
    Returns a cursor object. If an error is raised during connection,
    nothing is returned and the error will print to console.
    '''
    
    try:
        # Establish connection parameters
        connection = psycopg2.connect(user=user,
                                  password=pwd,
                                  host=host,
                                  port=port,
                                  database=database)
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query to find version info
        cursor.execute("SELECT version();")
        # Fetch and print result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        # reurns cursor object
        return cursor
    
    # If an error is raised, print details
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
        
def close_connection(cursor):
    
    '''
    Function to close active connection to PostGRE SQL server.
    Accepts:
        cursor (cursor object)
    Returns nothing.
    '''
    
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")
    
    
def execute(query, cursor):
    
    '''
    Executes query to active cursor. 
    Accepts:
        query (str)
        cursor (cursor object)
    Returns the result of the query.
    '''
    
    cursor.execute(query)
    record = cursor.fetchall()
    return record


def get_columns(table, cursor):
    
    '''
    Get column names for given table.
    Accepts:
        table (str)
        cursor (cursor object)
    Returns list of column names.
    '''
    
    column_names = execute(f"""SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    """, cursor)
    return [x[0] for x in column_names]

def get_table(table, cursor, columns='*', n_rows=5):
    
    '''
    Get subset of given table and given columns.
    Accepts:
        table(str)
        cursor(cursor object)
        columns(list of strings, default='*'): specific columns of the subset
        n_rows(int, default=5): amount of rows of data to pull
        
    Return dataframe of selected subset of table.
    '''
    
    data = execute(f'''SELECT {','.join(columns) if columns != '*' else columns} FROM {table} LIMIT {n_rows}''', cursor)
    
    if columns == '*':
        df = pd.DataFrame(data, columns=get_columns(table, cursor))
    else:
        df = pd.DataFrame(data, columns=columns)
    
    return df