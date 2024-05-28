import boto3
import configparser
import time

def read_aws_config(file_path='/home/almerco/datalakehouse/jupyter-notebooks/credentials/credentials'):
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read(file_path)

    # Access AWS configuration values
    aws_access_key_id = config.get('aws', 'aws_access_key_id')
    aws_secret_access_key = config.get('aws', 'aws_secret_access_key')
    region = config.get('aws', 'region')

    return aws_access_key_id, aws_secret_access_key, region

def create_boto3_session(access_key_id, secret_access_key, region):
    # Create a boto3 Session with the provided credentials
    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region
    )

    return session



def wait_for_query_completion(query_execution_id,athena_client):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state
        time.sleep(1)  # Wait for 1 second before checking again



def export_ddl_to_txt(database_name, output_folder, aws_session):
    # Create Glue client
    glue_client = aws_session.client('glue')
    athena_client = aws_session.client('athena')

    # Get list of table names in the database
    response = glue_client.get_tables(DatabaseName=database_name)
    table_names = [table['Name'] for table in response['TableList']]
    print(table_names)
    # Iterate over each table
    for table_name in table_names:
        # Get DDL for the table
        #if table_name = 'chatbot__tiempo_promedio_agente_bot':
        #    break
        
        # Define the Athena query
        query = f"SHOW CREATE TABLE {database_name}.{table_name}"
        
        # Execute the query
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': 'athena-bucket/'}
        )
        
        # Get query results
        query_execution_id = response['QueryExecutionId']
        # Wait for query to complete
        wait_for_query_completion(query_execution_id,athena_client)
        
        # Get query results
        result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        print(result_response)
        # Extract DDL from the query results
        ddl_rows = result_response['ResultSet']['Rows'][1:]
        ddl = '\n'.join([row['Data'][0]['VarCharValue'] for row in ddl_rows])
        
        # Write DDL to a text file
        output_file = f"{output_folder}/{table_name}.txt"

        # Create the text file
        #open(output_file, 'w').close()  # Create an empty text file

        with open(output_file, 'w') as f:
            f.write(f"CREATE EXTERNAL TABLE `{table_name}` (\n{ddl}")
        
        print(f"DDL for table {database_name}{table_name} exported to '{output_file}'")


access_key, secret_key, aws_region = read_aws_config()
aws_session = create_boto3_session(access_key, secret_key, aws_region)

# Example usage
database_name = 'db_hudi'
output_folder = f'./{database_name}'
export_ddl_to_txt(database_name, output_folder, aws_session)



import difflib

def compare_txt_files(file1_path, file2_path,i):
    # Read the content of the first file
    with open(file1_path, 'r') as file1:
        file1_content = file1.readlines()
    
    # Read the content of the second file
    with open(file2_path, 'r') as file2:
        file2_content = file2.readlines()
    
    # Perform the comparison
    differ = difflib.Differ()
    diff = list(differ.compare(file1_content, file2_content))
    
    # Print the differences
    if diff:
        print(f"{i}: Differences found between {file1_path} and {file2_path}:")
        for line in diff:
            if line.startswith(' '):
            #    or line.startswith('- CREATE EXTERNAL') or line.startswith('+ CREATE EXTERNAL')\
            #    or line.startswith('-   \'s3://us-west') or line.startswith('+   \'s3://us-west')\
            #    or line.startswith('-   \'trans') or line.startswith('+   \'trans') :
                continue
            print(line.strip())
    else:

        print("No differences found.")



my_dict = {
'tabla_1': 'tabla_1.1',

}

#file1_path = f'./develop_lakehouse_raw/{table_name}.txt'
#file2_path = f'./develop_lakehouse_master/{table_name}.txt'
#compare_txt_files(file1_path, file2_path)

# Iterate over the dictionary keys
i=1
for raw_table in my_dict:
    file1_path = f'./develop_lakehouse_raw/{raw_table}.txt'
    file2_path = f'./develop_lakehouse_master/{my_dict[raw_table]}.txt'

    print(f"raw_table: {file1_path}, master_table: {file2_path}\n")
    print(f"############################################################################################################################################################################\n")

    compare_txt_files(file1_path, file2_path, i)

    print(f"############################################################################################################################################################################\n")
    i+=1
    #if i == 2:
    #    break
