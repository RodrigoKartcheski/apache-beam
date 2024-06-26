/*DROP TABLE beegdata_dev_refined.dim_user;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_user(
user_id INTEGER,
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
idiom_id INTEGER,
idiom_name STRING,
idiom_code STRING,
fuso_name STRING,
login STRING,
acceptprivacy_datetime TIMESTAMP,
acceptprivacy_from STRING,
acceptprivacystore INTEGER,
hierarchy_leaderid INTEGER,
subdomain STRING,
mood_lastchange TIMESTAMP,
database STRING,
dateload TIMESTAMP,
op STRING,
sk_user BIGNUMERIC,
scd_started TIMESTAMP,
scd_finished TIMESTAMP,
scd_activate INTEGER)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = TRUE)