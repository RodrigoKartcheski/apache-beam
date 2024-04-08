###############################################################################################################
#########                                                                    importa os modulos                                                           #############
###############################################################################################################
import re
import json
import os
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime


###############################################################################################################
#########                                                  define os parametros mestres do pipeline                                              #############
###############################################################################################################

#imprimir local que o pipeline esta rodando
diretorio_atual = os.getcwd()
usuario = os.getlogin()
print("O diretório atual é:", diretorio_atual)
print("O usuário atual é:", usuario)



# define o modo de execução do pipeline
if "opt" in diretorio_atual:
    debug = True
    # Configurar as opções do pipeline
    pipeline_options = PipelineOptions()
    project = 'focus-mechanic-321819'
    dataset_trusted = 'beegdata_dev_trusted'
    dataset_refined = 'beegdata_dev_refined'
    destination_table = 'dim_user2'
    gcs_location = 'gs://beegdata_engineer/temp'
    print("Executando em modo de debug.")
else:
    debug = False
     # Configurar as opções do pipeline
    pipeline_options = PipelineOptions()
    project = 'focus-mechanic-321819'
    dataset_trusted = 'beegdata_prod_trusted'
    dataset_refined = 'beegdata_prod_refined'
    destination_table = 'dim_user2'
    gcs_location = 'gs://beegdata_engineer/temp'
    print("Executando em modo normal.")


load_tipe = "INCREMENTAL"    # incremental ou full

if load_tipe == "FULL":
    interval_time = 3000000            #horas
else:
    interval_time = 24                     #horas


###############################################################################################################
#########                                                                 define as funções reutilizáveis                                               #############
###############################################################################################################

def type_schema(field):

 

    doc_fields = {
        "<class 'str'>": 'STRING',
        "<class 'int'>": 'INTEGER',
        "<class 'bool'>": 'BOOLEAN',
        "<class 'datetime.date'>": 'DATE',
        "<class 'datetime'>": 'DATETIME',
        "<class 'float'>": 'FLOAT'
    }

    return doc_fields[f'{field}']

def schema(data):
    schema_bq = ''
    
    total_keys = len(data.keys())
    key_number = 1
    for key_name in data.keys():
        if total_keys == key_number:
            schema_bq += f'{key_name}:{type_schema(type(data[key_name]))}'
        else:
            schema_bq += f'{key_name}:{type_schema(type(data[key_name]))},'
        
        key_number += 1
    
    print(schema_bq)



###############################################################################################################
#########                                                                 define o pipeline de execução                                                #############
###############################################################################################################

# Criar o pipeline
pipeline = beam.Pipeline(options=pipeline_options)

client = bigquery.Client(project=project)
# Definir a consulta de merge
query = (f"""
MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
            SELECT 
                    u.id AS user_id,
                    u.team_id, 
                    u.usertype_id, 
                    u.userstatus_id, 
                    u.useravatar_id, 
                    u.template_id, 
                    u.agentid, 
                    u.externalid, 
                    u.username, 
                    u.name, 
                    u.lastname, 
                    u.email, 
                    u.place, 
                    u.cell, 
                    u.position, 
                    u.phone, 
                    u.branch, 
                    u.cellphone, 
                    u.description, 
                    u.password, 
                    u.img, 
                    u.newsletter, 
                    u.score, 
                    u.level, 
                    CAST(u.touruser AS TIMESTAMP) AS touruser, 
                    CAST(u.touradm AS TIMESTAMP) AS touradm, 
                    u.entrytime, 
                    u.exittime, 
                    u.acceptprivacy, 
                    u.blockdm, 
                    u.blockmobile, 
                    u.blockscale, 
                    u.changepassword, 
                    u.status, 
                    u.uac_id, 
                    u.cpf, 
                    u.leader, 
                    u.changeusername, 
                    u.mention, 
                    u.nid, 
                    CAST(u.created AS TIMESTAMP) AS created, 
                    u.created_by, 
                    CAST(u.firstaccess AS TIMESTAMP) AS firstaccess, 
                    CAST(u.lastaccess AS TIMESTAMP) AS lastaccess, 
                    u.idiom_id, 
                    id.code as idiom_code, 
                    u.fuso_name, 
                    u.mood_id, 
                    u.login, 
                    CAST(u.acceptprivacy_datetime AS TIMESTAMP) AS acceptprivacy_datetime, 
                    u.acceptprivacy_from, 
                    u.acceptprivacystore, 
                    hy.leaderid as hierarchy_leaderid, 
                    tm.subdomain, 
                    (SELECT max(mh.created_at) FROM {dataset_trusted}.mood_history mh where u.id = mh.user_id and u.team_id = mh.team_id and u.database = mh.database) as mood_lastchange,  
                    u.database, 
                    CAST(u.dateload AS TIMESTAMP) AS dateload,
					u.op,
					GENERATE_UUID() as sk_user
            FROM {dataset_trusted}.user as u
            INNER JOIN {dataset_trusted}.idiom as id ON u.database = id.database AND u.idiom_id = id.id
            LEFT JOIN {dataset_trusted}.hierarchy hy ON u.database = hy.database   AND u.id = hy.subordinateid 
            LEFT JOIN {dataset_trusted}.team as tm ON u.team_id = tm.id AND u.database = tm.database
                WHERE u.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
) AS S
ON (T.database = S.database AND T.user_id = S.user_id AND T.team_id = S.team_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
                    user_id = S.user_id,
                    team_id = S.team_id,
                    usertype_id = S.usertype_id,
                    userstatus_id = S.userstatus_id,
                    useravatar_id = S.useravatar_id,
                    template_id = S.template_id,
                    agentid = S.agentid,
                    externalid = S.externalid,
                    username = S.username,
                    name = S.name,
                    lastname = S.lastname,
                    email = S.email,
                    place = S.place,
                    cell = S.cell,
                    position = S.position,
                    phone = S.phone,
                    branch = S.branch,
                    cellphone = S.cellphone,
                    description = S.description,
                    password = S.password,
                    img = S.img,
                    newsletter = S.newsletter,
                    level = S.level,
                    touruser = S.touruser,
                    touradm = S.touradm,
                    entrytime = S.entrytime,
                    exittime = S.exittime,
                    acceptprivacy = S.acceptprivacy,
                    blockdm = S.blockdm,
                    blockmobile = S.blockmobile,
                    blockscale = S.blockscale,
                    changepassword = S.changepassword,
                    status = S.status,
                    uac_id = S.uac_id,
                    cpf = S.cpf,
                    leader = S.leader,
                    changeusername = S.changeusername,
                    mention = S.mention,
                    nid = S.nid,
                    created = S.created,
                    created_by = S.created_by,
                    firstaccess = S.firstaccess,
                    lastaccess = S.lastaccess,
                    idiom_id = S.idiom_id,
                    idiom_code = S.idiom_code,
                    fuso_name = S.fuso_name,
                    login = S.login,
                    acceptprivacy_datetime = S.acceptprivacy_datetime,
                    acceptprivacy_from = S.acceptprivacy_from,
                    acceptprivacystore = S.acceptprivacystore,
                    hierarchy_leaderid = S.hierarchy_leaderid,
                    subdomain = S.subdomain,
                    mood_lastchange = S.mood_lastchange,
                    database = S.database,
                    dateload =S.dateload
WHEN NOT MATCHED THEN
  INSERT (
                    user_id,
                    team_id,
                    usertype_id,
                    userstatus_id,
                    useravatar_id,
                    template_id,
                    agentid,
                    externalid,
                    username,
                    name,
                    lastname,
                    email,
                    place,
                    cell,
                    position,
                    phone,
                    branch,
                    cellphone,
                    description,
                    password,
                    img,
                    newsletter,
                    level,
                    touruser,
                    touradm,
                    entrytime,
                    exittime,
                    acceptprivacy,
                    blockdm,
                    blockmobile,
                    blockscale,
                    changepassword,
                    status,
                    uac_id,
                    cpf,
                    leader,
                    changeusername,
                    mention,
                    nid,
                    created,
                    created_by,
                    firstaccess,
                    lastaccess,
                    idiom_id,
                    idiom_code,
                    fuso_name,
                    login,
                    acceptprivacy_datetime,
                    acceptprivacy_from,
                    acceptprivacystore,
                    hierarchy_leaderid,
                    subdomain,
                    mood_lastchange,
                    database,
                    dateload
                    )
  VALUES (  
                    S.user_id,
                    S.team_id,
                    S.usertype_id,
                    S.userstatus_id,
                    S.useravatar_id,
                    S.template_id,
                    S.agentid,
                    S.externalid,
                    S.username,
                    S.name,
                    S.lastname,
                    S.email,
                    S.place,
                    S.cell,
                    S.position,
                    S.phone,
                    S.branch,
                    S.cellphone,
                    S.description,
                    S.password,
                    S.img,
                    S.newsletter,
                    S.level,
                    S.touruser,
                    S.touradm,
                    S.entrytime,
                    S.exittime,
                    S.acceptprivacy,
                    S.blockdm,
                    S.blockmobile,
                    S.blockscale,
                    S.changepassword,
                    S.status,
                    S.uac_id,
                    S.cpf,
                    S.leader,
                    S.changeusername,
                    S.mention,
                    S.nid,
                    S.created,
                    S.created_by,
                    S.firstaccess,
                    S.lastaccess,
                    S.idiom_id,
                    S.idiom_code,
                    S.fuso_name,
                    S.login,
                    S.acceptprivacy_datetime,
                    S.acceptprivacy_from,
                    S.acceptprivacystore,
                    S.hierarchy_leaderid,
                    S.subdomain,
                    S.mood_lastchange,
                    S.database,
                    S.dateload
                    )
"""
)

# Executar a consulta no BigQuery
job = client.query(query)
job.result()  # Espera a conclusão da consulta

# Verificar se ocorreram erros durante a execução
if job.errors:
    raise RuntimeError("Erro durante a execução da consulta: {}".format(job.errors))
    
  
pipeline.run()  



###############################################################################################################
#########                                                                fim do pipeline de execução                                                   #############
###############################################################################################################

"""
CREATE TABLE beegdata_dev_refined.dim_user(
id INTEGER,
team_id INT64,
usertype_id INTEGER,
userstatus_id INTEGER,
useravatar_id INTEGER,
template_id INTEGER,
agentid INTEGER,
externalid STRING,
username STRING,
name STRING,
lastname STRING,
email STRING,
place STRING,
cell STRING,
position STRING,
phone STRING,
branch STRING,
cellphone STRING,
description STRING,
password STRING,
img STRING,
newsletter INTEGER,
score INTEGER,
level STRING,
touruser TIMESTAMP,
touradm TIMESTAMP,
entrytime STRING,
exittime STRING,
acceptprivacy INTEGER,
required_mood INTEGER,
blockdm INTEGER,
blockmobile INTEGER,
blockscale INTEGER,
changepassword INTEGER,
status INTEGER,
uac_id INTEGER,
cpf STRING,
leader INTEGER,
changeusername INTEGER,
mention INTEGER,
nid INTEGER,
created TIMESTAMP,
created_by INTEGER,
firstaccess TIMESTAMP,
lastaccess TIMESTAMP,
idiom_id INTEGER,
idiom_name STRING,
idiom_code STRING,
fuso_name STRING,
mood_id INTEGER,
login STRING,
acceptprivacy_datetime TIMESTAMP,
acceptprivacy_from STRING,
acceptprivacystore INTEGER,
hierarchy_leaderid INTEGER,
subdomain STRING,
mood_lastchange TIMESTAMP,
database STRING,
dateload TIMESTAMP)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database
OPTIONS (
    require_partition_filter = TRUE)
"""