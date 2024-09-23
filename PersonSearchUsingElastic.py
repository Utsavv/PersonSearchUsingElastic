import os
import pyodbc
import random
import datetime
from elasticsearch import Elasticsearch, helpers
import time
from colorama import Fore, Style

# Define server name as a global variable so it can be set once
server = 'localhost'
db_name = 'PersonSearchDB'

def create_database_if_not_exists(db_name):
    # Connection to 'master' for creating the database
    master_conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=master;Trusted_Connection=yes;'
    master_conn = pyodbc.connect(master_conn_str)

    # Set autocommit to True to disable transactions
    master_conn.autocommit = True

    master_cursor = master_conn.cursor()

    # Check if the database exists, if not, create it
    create_db_query = f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE {db_name};"
    master_cursor.execute(create_db_query)

    # Close master connection
    master_cursor.close()
    master_conn.close()

# Function to return SQL Server connection
def get_sql_connection(db_name):
    """
    Returns a SQL Server connection object for the given database name.
    If the database does not exist, the function connects to 'master' first to create it.
    
    Parameters:
    db_name (str): The name of the database to connect to.

    Returns:
    pyodbc.Connection: A connection object to the specified database.
    """    

    # Connection to the newly created or existing database
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;'
    return pyodbc.connect(conn_str)

def execute_SQL_Query(db_name, query, params=None):
    """
    Executes a SQL query on the given database. Uses the connection obtained from get_sql_connection.
    
    Parameters:
    db_name (str): The name of the database.
    query (str): The SQL query to execute.
    params (tuple): Parameters to pass to the query (optional).
    
    Returns:
    list: The result of the query if it's a SELECT query, otherwise None.
    """
    conn = get_sql_connection(db_name)
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    
    if query.strip().upper().startswith("SELECT"):
        result = cursor.fetchall()  # Fetch all results if it's a SELECT query
    else:
        conn.commit()
        result = None

    cursor.close()
    conn.close()
    
    return result


def setup_database_and_bulk_insert_data(db_name, record_count=1000, batch_size=100):
    # List of Indian first names, last names, cities, and states
    first_names = [
        'Rahul', 'Anjali', 'Amit', 'Pooja', 'Rajesh', 'Sneha', 'Vikram', 'Neha', 'Suresh', 'Sunita',
        'Arjun', 'Kiran', 'Ravi', 'Priya', 'Nikhil', 'Meera', 'Kunal', 'Rina', 'Aakash', 'Divya',
        'Sanjay', 'Anita', 'Deepak', 'Kavita', 'Manish', 'Shweta', 'Rohit', 'Preeti', 'Vijay', 'Swati',
        'Ajay', 'Nisha', 'Gaurav', 'Shalini', 'Alok', 'Tanvi', 'Varun', 'Shruti', 'Vivek', 'Rashmi'
    ]
    last_names = [
        'Sharma', 'Patel', 'Gupta', 'Mehta', 'Jain', 'Agarwal', 'Reddy', 'Singh', 'Kumar', 'Verma',
        'Chopra', 'Desai', 'Iyer', 'Joshi', 'Kapoor', 'Malhotra', 'Nair', 'Pandey', 'Rao', 'Saxena',
        'Bose', 'Chatterjee', 'Das', 'Mukherjee', 'Banerjee', 'Bhat', 'Pillai', 'Menon', 'Choudhury', 'Trivedi',
        'Shah', 'Parekh', 'Chauhan', 'Patil', 'Dutta', 'Nayar', 'Kulkarni', 'Bhattacharya', 'Hegde', 'Sinha'
    ]
    cities = [
        'Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad', 'Ahmedabad', 'Kolkata', 'Pune', 'Jaipur', 'Lucknow',
        'Surat', 'Kanpur', 'Nagpur', 'Indore', 'Thane', 'Bhopal', 'Visakhapatnam', 'Patna', 'Vadodara', 'Ghaziabad'
    ]
    states = [
        'MH', 'DL', 'KA', 'TN', 'TS', 'GJ', 'WB', 'MH', 'RJ', 'UP',
        'MP', 'AP', 'BR', 'HR', 'PB', 'KL', 'OR', 'AS', 'JK', 'CH'
    ]

    def random_name():
        return random.choice(first_names), random.choice(last_names)

    def random_email(first_name, last_name):
        return f"{first_name.lower()}.{last_name.lower()}@randommail.com"

    def random_DOB():
        start_date = datetime.date(1950, 1, 1)
        end_date = datetime.date(2005, 12, 31)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)
        return random_date

    def random_zipcode():
        return str(random.randint(100000, 999999))  # Indian zip codes are 6 digits

    # Create Persons table using execute_SQL_Query
    create_table_query = '''
    IF OBJECT_ID('Persons', 'U') IS NOT NULL DROP TABLE Persons;
    CREATE TABLE Persons (
        FirstName NVARCHAR(50),
        LastName NVARCHAR(50),
        PreferredName NVARCHAR(50),
        City NVARCHAR(50),
        State NVARCHAR(50),
        ZipCode NVARCHAR(10),
        DOB DATE,
        Email NVARCHAR(100)
    );
    '''
    execute_SQL_Query(db_name, create_table_query)

    # Insert records in batches
    for batch_start in range(0, record_count, batch_size):
        values = []
        for _ in range(batch_size):
            first_name, last_name = random_name()
            preferred_name = first_name  # Assume preferred name is the first name
            city = random.choice(cities)
            state = random.choice(states)
            zipcode = random_zipcode()
            dob = random_DOB()
            dob_str = dob.strftime('%Y-%m-%d')
            email = random_email(first_name, last_name)
            values.append(f"SELECT '{first_name}', '{last_name}', '{preferred_name}', '{city}', '{state}', '{zipcode}', '{dob_str}', '{email}'")

        # Create bulk insert query using INSERT INTO ... SELECT
        insert_query = '''
        INSERT INTO Persons (FirstName, LastName, PreferredName, City, State, ZipCode, DOB, Email)
        ''' + " UNION ALL ".join(values)

        execute_SQL_Query(db_name, insert_query)
        print(f"Inserted batch starting at record {batch_start}")

#3. Creating an Elasticsearch Index
#	• Define Mappings: Specify how each field should be indexed and analyzed, particularly those requiring fuzzy search capabilities.
#Index Creation: Use Python scripts or tools like Kibana to create the index with the defined mappings.

from elasticsearch import Elasticsearch, helpers
import urllib3

# Suppress warnings about insecure connections (optional)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

   
# Replace with your actual password
elastic_password = "IMoWOv8DHTMNnQod37NS"

# Initialize the Elasticsearch client with SSL and authentication
# make sure elastic search is running on port 9200, 
# Use the following command to start elastic search
# .\elasticsearch-8.11.1\bin\elasticsearch.bat


es = Elasticsearch(
    ["https://localhost:9200"],
    ca_certs=False,          # Disable SSL certificate verification
    verify_certs=False,      # Disable SSL cert verification (use with caution)
    basic_auth=("elastic", elastic_password),
)

def index_data_to_elasticsearch(db_name, index_name, batch_size=10000):
    
    # Delete the existing index if it exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    # Create a new index with mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "FirstName": {"type": "text"},
                "LastName": {"type": "text"},
                "PreferredName": {"type": "text"},
                "City": {"type": "text"},
                "State": {"type": "keyword"},
                "ZipCode": {"type": "keyword"},
                "DOB": {"type": "date"},
                "Email": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index=index_name, body=index_mappings)
    print(f"Created new index: {index_name}")

    # Get total number of records
    total_records_query = "SELECT COUNT(*) FROM Persons"
    total_records_result = execute_SQL_Query(db_name, total_records_query)
    total_records = total_records_result[0][0]

    offset = 0
    while offset < total_records:
        query = f'''
        SELECT FirstName, LastName, PreferredName, City, State, ZipCode, DOB, Email
        FROM Persons
        ORDER BY FirstName
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
        '''
        rows = execute_SQL_Query(db_name, query)
        actions = []
        for row in rows:
            print(f"\n\nrow: {row}")
            doc = {
                "_index": index_name,
                "_source": {
                    "FirstName": row[0],
                    "LastName": row[1],
                    "PreferredName": row[2],
                    "City": row[3],
                    "State": row[4],
                    "ZipCode": row[5],
                    "DOB": row[6].strftime('%Y-%m-%d') if row[6] else None,
                    "Email": row[7]
                }
            }
            actions.append(doc)
            
        try:
            helpers.bulk(es, actions)
        except helpers.BulkIndexError as e:
            print(f"Bulk indexing error: {e}")
            for error in e.errors:
                print(error)
        
        offset += batch_size
        print(f"Indexed {offset}/{total_records} records")


'''
5. Implementing Search Queries
	• Basic Searches: Perform searches on single or multiple fields.
	• Fuzzy Searches: Utilize Elasticsearch's fuzzy query capabilities to handle misspellings and partial matches.
	• Complex Queries: Combine multiple search criteria using boolean operators.
'''

def ExecuteElasticSearch(search_query):
    batch_size=10
    # Execute the search
    response = es.search(index="people_index", body=search_query, size=batch_size)
    
    # Process the results
    print(f"Found {response['hits']['total']['value']} documents:")
    for hit in response['hits']['hits']:
        print(f"\n\n\n{hit['_source']}")


        
def first_name_search(column_name, first_name):
    search_query = {
        "query": {
            "match": {
                column_name: first_name
            }
        }
    }
    ExecuteElasticSearch(search_query)

def multi_field_wildcard_search(first_name, last_name, birth_year):    
    search_query = {
        "query": {
            "bool": {
                "must": [
                    {"wildcard": {"FirstName": f"*{first_name}*"}},
                    {"wildcard": {"LastName": f"*{last_name}*"}},
                    {
                        "range": {
                            "BirthDate": {
                                "gte": f"{birth_year}-01-01",
                                "lte": f"{birth_year}-12-31"
                            }
                        }
                    }
                ]
            }
        }
    }
    ExecuteElasticSearch(search_query)

def fuzzy_logic_search(first_name):
    search_query = {
        "query": {
            "fuzzy": {
                "FirstName": {
                    "value": first_name,
                    "fuzziness": 2
                }
            }
        }
    }    
    ExecuteElasticSearch(search_query)
    
def boolean_logic_search(first_name, last_name, birth_year):
    search_query = {
        "query": {
            "bool": {
                "should": [
                    {"wildcard": {"FirstName": f"*{first_name}*"}},
                    {"wildcard": {"LastName": f"*{last_name}*"}},
                    {
                        "range": {
                            "BirthDate": {
                                "gte": f"{birth_year}-01-01",
                                "lte": f"{birth_year}-12-31"
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
    ExecuteElasticSearch(search_query)

def compare_performance(first_name, last_name, preferred_name, iterations=1000):
    # SQL query
    sql_query = '''
    SELECT FirstName, LastName, PreferredName
    FROM Persons
    WHERE FirstName = ? AND LastName = ? AND PreferredName LIKE ?
    '''

    # Warm-up run for SQL Server
    execute_SQL_Query(db_name, sql_query, (first_name, last_name, preferred_name))

    # Record execution times for SQL Server
    sql_execution_times = []
    for _ in range(iterations):
        start_time = time.perf_counter()
        execute_SQL_Query(db_name, sql_query, (first_name, last_name, preferred_name))
        end_time = time.perf_counter()
        sql_execution_times.append(end_time - start_time)

    # Calculate average execution time for SQL Server
    average_time_sql = sum(sql_execution_times) / iterations

    # Elasticsearch query
    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"FirstName": first_name}},
                    {"match": {"LastName": last_name}},
                    {"wildcard": {"PreferredName": preferred_name}}
                ]
            }
        }
    }

    # Warm-up run for Elasticsearch
    es.search(index="person_index", body=es_query)

    # Record execution times for Elasticsearch
    es_execution_times = []
    for _ in range(iterations):
        start_time = time.perf_counter()
        es.search(index="person_index", body=es_query)
        end_time = time.perf_counter()
        es_execution_times.append(end_time - start_time)

    # Calculate average execution time for Elasticsearch
    average_time_es = sum(es_execution_times) / iterations

    print(f"\033[91mAverage SQL execution time: {average_time_sql:.6f} seconds\033[0m")
    print(f"\033[92mAverage Elasticsearch execution time: {average_time_es:.6f} seconds\033[0m")
    performance_gain = (average_time_sql - average_time_es) / average_time_sql * 100
    print(f"\033[93mElasticsearch performance gain: {performance_gain:.2f}%\033[0m")
#Master Database Creation
create_database_if_not_exists(db_name)

#Setup Database and Bulk Insert Data  
setup_database_and_bulk_insert_data(db_name, record_count=1000000, batch_size=10000)

#Index Data to Elastic Search
index_data_to_elasticsearch('PersonSearchDB', 'person_index', batch_size=10000)


# Example usage of search functions
first_name_search("FirstName", "Rahul")

#Multi Field Wildcard Search
multi_field_wildcard_search("Rahul", "Sharma", 1990)

#Fuzzy Logic Search
fuzzy_logic_search("Rahul")

#Boolean Logic Search
boolean_logic_search("Rahul", "Sharma", 1990)

#Performance Comparison
compare_performance("Rahul", "Sharma", "Rahul%")


#Drop Database for Cleanup
def drop_database(db_name):
    execute_SQL_Query('master', f"DROP DATABASE IF EXISTS {db_name}")    
    
drop_database(db_name)