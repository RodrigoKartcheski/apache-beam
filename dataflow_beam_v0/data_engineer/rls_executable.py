#import bigquery
from google.cloud import bigquery
import pandas as pd

df = pd.read_csv('/home/rodrigo/opt/apache_beam/beedoo/beegdata/data_engineer/table_rls.csv', delimiter='|')


# Itera sobre as linhas do DataFrame
for index, row in df.iterrows():
    # Imprime o índice e os valores da linha
    project_id = row["project_id"]
    dataset_id = row["dataset_id"]
    dataset_schema = row["dataset_schema"]
    table_id = row["table_id"]
    police_name = row["police_name"]
    grant_to = row["grant_to"]
    filter_using = row["filter_using"]
    
    print(f""" crated RLS NAME - {police_name} | project_id: {project_id} | dataset_id: {dataset_id}  | table_id: {table_id} | grant_to: {grant_to} | filter_using: {filter_using}""")
    
    client = bigquery.Client()
      
    query = f"""
    GRANT `roles/bigquery.dataViewer`,`roles/bigquery.user`,`roles/bigquery.filteredDataViewer`
    ON SCHEMA  `{project_id}.{dataset_schema}`
    TO {grant_to};
    
    CREATE OR REPLACE ROW ACCESS POLICY {police_name}
    ON `{project_id}.{dataset_id}.{table_id}`
    GRANT TO ({grant_to})
    FILTER USING ({filter_using});
    """.format(dataset_id, table_id)

    job_config = bigquery.QueryJobConfig()

    job = client.query(query, job_config=job_config)

    job.result()

