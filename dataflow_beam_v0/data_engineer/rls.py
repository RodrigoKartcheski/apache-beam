#import bigquery
from google.cloud import bigquery

#project_id = "focus-mechanic-321819"
#dataset_id = "beegdata_dev_refined"
#dataset_id = "beegdata_prod_mapfre"
#table_id = "user"
#police_name = "rls_"
#grant_to = '''"user:beegdata.actionline@beedoo.com.br","serviceAccount:actionline-beegdata@focus-mechanic-321819.iam.gserviceaccount.com"'''
#filter_using = '''database = 'beedoo' and team_id IN (749)'''
#user_id = "my_user"

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

