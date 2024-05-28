import boto3

def validate_glue_table_data(glue_database, glue_table):
    # Specify your AWS region
    region = 'your_region'

    # Create a Glue client
    glue_client = boto3.client('glue', region_name=region)

    # Create an Athena client
    athena_client = boto3.client('athena', region_name=region)

    try:
        # Get the metadata of the Glue table
        response = glue_client.get_table(DatabaseName=glue_database, Name=glue_table)
        table_metadata = response['Table']

        # Check if the 'StorageDescriptor' key exists
        if 'StorageDescriptor' in table_metadata:
            storage_descriptor = table_metadata['StorageDescriptor']

            # Check if the 'Columns' key exists
            if 'Columns' in storage_descriptor:
                columns = storage_descriptor['Columns']

                for column in columns:
                    column_name = column['Name']
                    column_data_type = column['Type']
                    is_primary_key = column.get('isPrimaryKey', False)

                    # Validate consistency (column data type)
                    print(f"Validating data type for column '{column_name}'...")
                    validate_data_type(glue_database, glue_table, column_name, column_data_type)

                    # Validate completeness (null values)
                    print(f"Validating null values for column '{column_name}'...")
                    validate_null_values(glue_database, glue_table, column_name)

                    # Validate integrity (isPrimaryKey)
                    if is_primary_key:
                        print(f"Validating primary key for column '{column_name}'...")
                        validate_primary_key(glue_database, glue_table, column_name)
                    else:
                        print(f"Column '{column_name}' is not a primary key.")

            else:
                print("No 'Columns' key found in StorageDescriptor.")
        else:
            print("No 'StorageDescriptor' key found in table metadata.")

    except Exception as e:
        print(f"Error: {str(e)}")

def validate_data_type(glue_database, glue_table, column_name, expected_data_type):
    # Query Athena to validate data type
    query = f"SELECT COUNT(*) FROM {glue_database}.{glue_table} WHERE TRY_CAST({column_name} AS {expected_data_type}) IS NULL"
    result = execute_athena_query(query)
    
    if result == 0:
        print(f"Data type for column '{column_name}' is consistent.")
    else:
        print(f"Data type mismatch found in column '{column_name}'.")

def validate_null_values(glue_database, glue_table, column_name):
    # Query Athena to validate null values
    query = f"SELECT COUNT(*) FROM {glue_database}.{glue_table} WHERE {column_name} IS NULL"
    result = execute_athena_query(query)
    
    if result == 0:
        print(f"No null values found in column '{column_name}'.")
    else:
        print(f"Null values found in column '{column_name}'.")

def validate_primary_key(glue_database, glue_table, column_name):
    # Query Athena to validate primary key integrity
    query = f"SELECT COUNT(*), {column_name} FROM {glue_database}.{glue_table} GROUP BY {column_name} HAVING COUNT(*) > 1"
    result = execute_athena_query(query)
    
    if result == 0:
        print(f"Primary key for column '{column_name}' is consistent.")
    else:
        print(f"Duplicate values found in primary key column '{column_name}'.")

def execute_athena_query(query):
    # Execute Athena query and return the result count
    athena_client = boto3.client('athena', region_name=region)
    query_execution = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': glue_database},
        ResultConfiguration={'OutputLocation': 's3://your-athena-query-output-location/'}
    )
    execution_id = query_execution['QueryExecutionId']
    
    # Wait for the query to complete
    response = None
    while response is None or response['QueryExecution']['Status']['State'] == 'RUNNING':
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
    
    # Get the result count
    result_count = int(response['QueryExecution']['ResultConfiguration']['OutputLocation'].split('/')[-1].split('-')[-1])
    return result_count

# Example usage
if __name__ == "__main__":
    # Replace with your Glue database and table
    glue_database = "your_glue_database"
    glue_table = "your_glue_table"

    validate_glue_table_data(glue_database, glue_table)
