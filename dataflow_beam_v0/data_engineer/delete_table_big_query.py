from google.cloud import bigquery
import pandas as pd

#df = pd.read_csv('gs://beegdata_engineer/metadados/refined_controller.csv', delimiter=',')


# Itera sobre as linhas do DataFrame
for index, row in df.iterrows():
   # Imprime o índice e os valores da linha
    project_id = row["project_id"]
    dataset_id = row["dataset_id"]
    table_id = row["table_id"]

    print(f""" Excluindo a tabela - project_id: {project_id} | dataset_id: {dataset_id}  | table_id: {table_id}""")

    client = bigquery.Client()
  
    query = f"""DROP TABLE {project_id }.{dataset_id}.{table_id}
    """.format(project_id, dataset_id, table_id)

    job_config = bigquery.QueryJobConfig()

    job = client.query(query, job_config=job_config)

    job.result()

