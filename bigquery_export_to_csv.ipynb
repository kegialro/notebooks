import csv
import gzip
import shutil
from google.cloud import bigquery
from google.oauth2 import service_account
 
# Path to your .json key file
credentials_path = './credenciales.json'
 
# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    credentials_path,
)
 
# Create a BigQuery client using the credentials
client = bigquery.Client(credentials=credentials)
esquema='gestion_bancaria_data'
tabla='CLIENTES'
# Ejecutar la consulta
query = f"SELECT * FROM `xyz.{esquema}.{tabla}`"
query_job = client.query(query)
print(query_job.result())
# Guardar los resultados en un archivo CSV
with open(f'part-00001-{esquema}_{tabla}.csv', 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter='¦')

    # Escribir encabezados (nombres de columnas)
    headers = [field.name for field in query_job.result().schema]
    csvwriter.writerow(headers)

    # Escribir filas de datos
    for row in query_job.result():
        csvwriter.writerow(row.values())

# Comprimir el archivo CSV en formato gzip
with open(f'part-00001-{esquema}_{tabla}.csv', 'rb') as f_in:
    with gzip.open(f'part-00001-{esquema}_{tabla}.csv.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


-------------------------------------------------------------------------


import csv
import gzip
import shutil
from google.cloud import bigquery
from google.oauth2 import service_account
 
# Path to your .json key file
credentials_path = './credenciales.json'
 
# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    credentials_path,
)
 
# Create a BigQuery client using the credentials
client = bigquery.Client(credentials=credentials)
dataset_name='gestion_bancaria_data'
table_name='xx'
 
# Get the table
table_ref = client.dataset(dataset_name).table(table_name)
table = client.get_table(table_ref)
 
# Print the schema
for field in table.schema:
    print(f'`{field.name.lower()}` {field.field_type.lower()},')

-------------------------------------------------------------
