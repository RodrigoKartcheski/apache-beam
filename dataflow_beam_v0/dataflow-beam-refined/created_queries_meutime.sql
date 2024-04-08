DROP TABLE IF EXISTS beegdata_dev_refined.dim_user;
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
restricted_user INTEGER,
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
    require_partition_filter = FALSE);
# nota 1: obrigatoriedade do filtro de partição tirada a pedido do cliente
	


DROP TABLE IF EXISTS beegdata_dev_refined.dim_groupuser;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_groupuser(
					rnk INTEGER,
					sk_groupuser BIGNUMERIC,
					group_id INTEGER,
					group_name STRING,
					user_id INTEGER,
					user_name STRING,
					team_id INTEGER,
                    database STRING,
                    dateload TIMESTAMP,
					op STRING,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = TRUE);
	
	


DROP TABLE IF EXISTS beegdata_dev_refined.dim_complementaryfields;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_complementaryfields(
				    user_id INTEGER,
				    team_id INTEGER,
				    customfield_id INTEGER,
				    customfield_name STRING,
				    customfield_type STRING,
				    customfield_value STRING,
				    customfield_name_clean STRING,
				    cdc_commit_date DATE,
                    database STRING,
                    dateload TIMESTAMP,
					sk_complementaryfields BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = TRUE);
	
	
CREATE OR REPLACE TABLE `beegdata_dev_refined.dim_instructor` (

  instructor_id INTEGER,
  instructor_name STRING,
  database STRING,
  sk_instructor BIGNUMERIC,
  scd_started TIMESTAMP,
  scd_finished TIMESTAMP,
  scd_activate INTEGER
)
	
	
	
	
	
	
	
	
	
	
	
	
	
DROP TABLE IF EXISTS beegdata_dev_refined.fact_usermood;
CREATE OR REPLACE TABLE beegdata_dev_refined.fact_usermood(
idmood INTEGER,
user_id INTEGER,
team_id INTEGER,
message STRING,
created_at TIMESTAMP,
title STRING,
status INTEGER,
score INTEGER,
firstaccess TIMESTAMP,
lastaccess TIMESTAMP,
dateaccess TIMESTAMP,
database STRING,
dateload TIMESTAMP,
op STRING,
sk_user BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = TRUE);
	

	




DROP TABLE  IF EXISTS beegdata_dev_refined.fact_useraccess;
CREATE OR REPLACE TABLE beegdata_dev_refined.fact_useraccess(
access_id INTEGER,
user_id INTEGER,
team_id INTEGER,
dateaccess TIMESTAMP,
database STRING,
dateload TIMESTAMP,
op STRING,
sk_user BIGNUMERIC,
sk_groupuser BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = FALSE);
# nota 1: obrigatoriedade do filtro de partição tirada a pedido do cliente
	

MERGE INTO `focus-mechanic-321819.beegdata_dev_refined.dim_porgrupo` AS T
USING (SELECT
  G.user_id,

  GPING.team_id,

  GVAL.group_id,

  ARRAY_AGG(STRUCT(
    GPING.group_name AS grouping_name,

    GOPT.value AS groupingoption_value,

    GPING.number AS grouping_number,

    H.name AS level_hierarchy
  )) AS ging_1,

  GPING.database,
  CAST(CONCAT(G.user_id, FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())) AS BIGNUMERIC) AS sk_porgrupo

FROM `focus-mechanic-321819.beegdata_dev_trusted.grouping` GPING

INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupingoptions` GOPT ON GOPT.grouping_id = GPING.id AND GOPT.database = GPING.database

INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupingvalues` AS GVAL ON GVAL.groupingoption_id = GOPT.id AND GVAL.database = GOPT.database

LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.group` G ON G.id = GVAL.group_id AND G.database = GVAL.database

LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.higherbeneathtype` H ON H.nid = GPING.number AND H.database = GPING.database

GROUP BY
  G.user_id,
  GPING.team_id,
  GVAL.group_id,
  GPING.database,
  GPING.dateload) AS S

ON (T.database = S.database AND T.group_id = S.group_id AND T.user_id = S.user_id AND T.team_id > 0)

WHEN MATCHED THEN
  UPDATE SET
					group_id = S.group_id,
					user_id = S.user_id,
					team_id = S.team_id,
					database = S.database,
					scd_finished = CURRENT_TIMESTAMP(),
					scd_activate = 0
WHEN NOT MATCHED THEN
  INSERT (
					sk_porgrupo,
					group_id,
					user_id,
					team_id,
          ging,
          database,
					scd_started,
					scd_finished,
					scd_activate
  )
  VALUES (  
					S.sk_porgrupo,
					S.group_id,
					S.user_id,
					S.team_id,
          S.ging_1,
          S.database,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
);



MERGE INTO beegdata_dev_refined.dim_instructor AS T

USING (

  SELECT query.*, CAST(CONCAT(query.instructor_id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(query.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(query.dateload AS DATE), query.instructor_id)) AS BIGNUMERIC) AS sk_instructor FROM (

    SELECT DISTINCT

      lc.instructor_id,

      u.name as instructor_name,

      u.database,

      u.dateload

    FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclass` as lc

    INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.user` as u ON u.id = lc.instructor_id and u.database = lc.database

  ) AS query

) AS S

ON T.instructor_id = S.instructor_id AND T.database = S.database

 

WHEN MATCHED THEN UPDATE SET

  T.instructor_id = S.instructor_id,

  T.database = S.database,

  T.instructor_name = S.instructor_name,

  scd_finished = CURRENT_TIMESTAMP(),

  scd_activate = 0  

WHEN NOT MATCHED THEN INSERT

(

  instructor_id,

  database,

  instructor_name,

  sk_instructor,

  scd_started,

  scd_finished,

  scd_activate

) VALUES (

  S.instructor_id,

  S.database,

  S.instructor_name,

  S.sk_instructor,

  CURRENT_TIMESTAMP(),

  '1900-01-01 00:00:00',

  1

)





