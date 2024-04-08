DROP TABLE IF EXISTS beegdata_dev_refined.dim_category;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_category(			
					category_id INTEGER, 
					category STRING,
					user_id INTEGER, 
					team_id INTEGER, 
					database STRING,
					dateload TIMESTAMP,
					sk_category BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = TRUE);
	


DROP TABLE IF EXISTS beegdata_dev_refined.dim_learnclass;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_learnclass(			
      		learn_id INTEGER,
				  learnclass_id INTEGER,
					--user_id INTEGER,
					drop_name STRING,
					learn_description STRING,
					learn_status INTEGER,
					team_id INTEGER,
					classname STRING,
					class_description STRING,
					created_date TIMESTAMP,
					created_class TIMESTAMP,
				    by_groups INTEGER,
					average_time_finish INTEGER,
					selective_process INTEGER,
					publishin_date TIMESTAMP,
					finishin_date TIMESTAMP,
					class_status INTEGER,
					class_type INTEGER,
					database STRING, 
					dateload TIMESTAMP,
					--op STRING,
					sk_learnclass BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = TRUE);
	

DROP TABLE IF EXISTS beegdata_dev_refined.dim_students;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_students(
					learn_id	 INTEGER,
					learnclass_id INTEGER,
					user_id INTEGER,
					by_groups INTEGER,
					team_id INTEGER,
					group_id STRING,
					group_name STRING,
					database STRING, 
					sk_students BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, user_id
  OPTIONS (
    require_partition_filter = TRUE, description = 'Example name and addresses table');

DROP TABLE IF EXISTS beegdata_dev_refined.dim_students2;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_students2(	
					learn_id	 INTEGER,
					learnclass_id INTEGER,
					user_id INTEGER,
					by_groups INTEGER,
					team_id INTEGER,
				    groupuser ARRAY<STRUCT <
					    group_id INTEGER,
					    group_name STRING
			        >>,
					database STRING, 
					sk_students BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, user_id
  OPTIONS (
    require_partition_filter = TRUE, description = 'Example name and addresses table');

/*
SELECT
  user_id, learnclass_id,
  a.group_id,
  a.group_name
FROM
   `focus-mechanic-321819.beegdata_dev_refined.dim_students2` CROSS JOIN UNNEST(groupuser) AS a
   where team_id > 0
   and user_id = 137971
   order by user_id, learnclass_id, a.group_id;
*/


	
	

	
	
	
DROP TABLE IF EXISTS beegdata_dev_refined.fact_learnquestions;
CREATE OR REPLACE TABLE beegdata_dev_refined.fact_learnquestions(
	learnquestions_id INTEGER,
	learn_id INTEGER,
	learnclass_id INTEGER,
	user_id INTEGER,
	team_id INTEGER,
	by_groups INTEGER,
	question STRING,
	response STRING,
	correct_answers STRING,
	difficulty STRING,
	response_date TIMESTAMP,
	database STRING, 
    dateload TIMESTAMP,
	op STRING,
	sk_learnclass BIGNUMERIC,
	sk_user BIGNUMERIC,
	sk_instructor BIGNUMERIC,
	sk_students BIGNUMERIC,
	sk_category BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = FALSE, description = 'Example name and addresses table');
    
	
	

DROP TABLE IF EXISTS  beegdata_dev_refined.facta_learnstatus;
CREATE OR REPLACE TABLE beegdata_dev_refined.facta_learnstatus(
	learn_id INTEGER,
	learnclass_id INTEGER,
	user_id INTEGER,
	team_id INTEGER,
	--by_groups INTEGER,
	goal_type STRING,
	questions INTEGER,
	answers INTEGER,
	correct_answers INTEGER,
	assertiveness INTEGER,
	score_obtained INTEGER,
	points INTEGER,
	goal INTEGER,
	result STRING,
	response_date TIMESTAMP,
	attempts INTEGER,
	attempt INTEGER,
	remaining_treis STRING,
	result_attempts STRING,
	database STRING, 
    dateload TIMESTAMP,
	sk_learnclass BIGNUMERIC,
	sk_user BIGNUMERIC,
	sk_instructor BIGNUMERIC,
	sk_students BIGNUMERIC,
	sk_category BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = FALSE, description = 'Example name and addresses table');