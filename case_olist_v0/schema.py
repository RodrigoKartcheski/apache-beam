from apache_beam.io.avroio import ReadFromAvro
import sys
import glob
import traceback
import fastavro
from fastavro import parse_schema
import logging
#import json
#from google.cloud import storage
#import sys
#import traceback
#import itertools
#import time

debug = True
#variaveis de execução
if debug:
    collection_read = 'raw_user'
    table_controller = "gs://beegdata_engineer/metadados/trusted_controller.csv"

    df = pd.read_csv(table_controller)
    #df_filtered = df[df['target_table_name'] == collection_read]
    df_filtered = df[(df['target_table_name'] == collection_read) & (df['load_controller'] == 1)]
    field_mapping_str = df_filtered['field_mapping'].iloc[0]
    field_mapping = eval(field_mapping_str)
    field_mapping = [(t[0], 'integer') if t[1] == 'int' or t[1] == 'bigint' else t for t in field_mapping]
    mapping = ', '.join([f"{t[0]}:{t[1].upper()}" for t in field_mapping])
    mapp = [bigquery.SchemaField(t[0], t[1].upper()) for t in field_mapping]
    #mapping_simp = [f"bigquery.SchemaField(\"{field.name}\", \"{field.field_type}\")" for field in mapp] # Cria Schema simplificado do Big Query
    mapping_bq = [bigquery.SchemaField(field.name, field.field_type) for field in mapp] # Cria Schema simplificado do Big Query
    mapping_evol = [bigquery.SchemaField(t[0], t[1].upper()) for t in field_mapping]   # Cria Schema evoluido do Big Query
    enginner_bucket = "gs://beegdata_engineer"
    transient_bucket = df_filtered['transient_bucket'].iloc[0]
    raw_project="focus-mechanic-321819"
    raw_dataset = df_filtered['raw_dataset'].iloc[0]
    source_collection = df_filtered['source_table_name'].iloc[0]
    #target_collection = df_filtered['target_table_name'].iloc[0]
    target_collection = "raw_user"
    primary_key = df_filtered['primary_key'].iloc[0]
    temporay_key = primary_key.split(", ")
    partition_field = df_filtered['partition_field'].iloc[0]
    #primary_key = "coluna1"
    field_max_date = "cdc_commit_timestamp"
    database = "beedoo"
