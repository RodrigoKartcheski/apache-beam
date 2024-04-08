MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT
					learn_id,
					learnclass_id,
					drop_name,
					learn_description,
					learn_status,
					team_id,
					classname,
					class_description,
					created_date,
					created_class,
					by_groups,
					average_time_finish,
					selective_process,
					publishin_date,
					finishin_date,
					class_status,
					class_type,
					database,
					dateload,
					CAST(CONCAT(learn_id, learnclass_id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(dateload AS DATE))), ROW_NUMBER() OVER(PARTITION BY learnclass_id ORDER BY CAST(dateload AS DATE), learnclass_id)) AS BIGNUMERIC) AS sk_learnclass
					FROM (
								SELECT DISTINCT
											IFNULL(l.id,-1) AS learn_id,
											IFNULL(lc.id,-1) AS learnclass_id,
											--IFNULL(lu.user_id,-1) AS user_id,
											l.title AS drop_name,
											l.description AS learn_description,
											l.status AS learn_status,
											l.team_id,
											lc.name as classname,
											lc.description AS class_description,
											l.created AS created_date,
											lc.created AS created_class,
											lc.by_groups,
											l.average_time_finish,
											l.selective_process,
											lc.publishin as publishin_date,
											lc.finishin as finishin_date,
											lc.status as class_status,
											lc.type as class_type,
											l.database, 
											CAST(lc.dateload AS TIMESTAMP) AS dateload
								FROM beegdata_dev_trusted.learn l
								INNER JOIN beegdata_dev_trusted.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
								INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.database = lu.database AND lc.id = lu.learnclass_id
								INNER JOIN beegdata_dev_trusted.learndrop ld ON l.database = ld.database AND l.id = ld.learn_id
									WHERE lu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3000 HOUR)
			) AS query
) AS S

ON (T.database = S.database AND T.learn_id = S.learn_id AND T.learnclass_id = S.learnclass_id AND T.team_id > 0)
WHEN MATCHED  AND T.dateload < S.dateload THEN
  UPDATE SET 
					learn_id = S.learn_id,
					learnclass_id = S.learnclass_id,
					drop_name = S.drop_name,
					learn_description = S.learn_description,
					learn_status = S.learn_status,
					team_id = S.team_id,
					classname = S.classname,
					class_description = S.class_description,
					created_date = S.created_date,
					created_class = S.created_class,
					by_groups = S.by_groups,
					average_time_finish = S.average_time_finish,
					selective_process = S.selective_process,
					publishin_date = S.publishin_date,
					finishin_date = S.finishin_date,
					class_status = S.class_status,
					class_type = S.class_type,		
                    database = S.database,
					scd_finished = CURRENT_TIMESTAMP(),
					scd_activate = 0
WHEN NOT MATCHED THEN
  INSERT (
					learn_id,
					learnclass_id,
					drop_name,
					learn_description,
					learn_status,
					team_id,
					classname,
					class_description,
					created_date,
					created_class,
					by_groups,
					average_time_finish,
					selective_process,
					publishin_date,
					finishin_date,
					class_status,
					class_type,					
                    database,
                    dateload,
					sk_learnclass,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
  					S.learn_id,
					S.learnclass_id,
					S.drop_name,
					S.learn_description,
					S.learn_status,
					S.team_id,
					S.classname,
					S.class_description,
					S.created_date,
					S.created_class,
					S.by_groups,
					S.average_time_finish,
					S.selective_process,
					S.publishin_date,
					S.finishin_date,
					S.class_status,
					S.class_type,	
                    S.database,
                    S.dateload,
					S.sk_learnclass,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )	
	
	
	
	
	
	
	



