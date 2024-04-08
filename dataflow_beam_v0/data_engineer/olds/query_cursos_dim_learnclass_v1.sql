MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT DISTINCT
						l.id AS learn_id,
						lc.id AS learnclass_id,
						lu.user_id,
						l.title AS drop_name,
						l.description AS learn_description,
						lc.status,
						l.team_id,
						lc.name as classname,
						lc.description AS class_description,
						#classtype as class_mode   -----------------------------------------VINHA DA LEARNDETAILL ALL
						# created_date ------------------------------------------------------VEM DE QUAL TABELA?
						# created_class  -----------------------------------------VINHA DA LEARNDETAILL ALL
						# sclass_status   -----------------------------------------VINHA DA LEARNDETAILL ALL
						# fast_result # sclass_status   -----------------------------------------VINHA DA LEARNDETAILL ALL REPETIDO
						lc.by_groups,
						l.average_time_finish,
						l.selective_process,
						lc.publishin as publishin_date,
						lc.finishin as finishin_date,
						lc.status as class_status,
						lc.type as class_type,
						lu.database, 
						CAST(lu.dateload AS TIMESTAMP) AS dateload,
						lu.op,
						CAST(CONCAT(l.id, lc.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(lu.dateload AS DATE))), ROW_NUMBER() OVER(PARTITION BY lu.user_id ORDER BY CAST(lu.dateload AS DATE), lu.id)) AS BIGNUMERIC) AS sk_learnclass
			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.database = lu.database AND lc.id = lu.learnclass_id
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.database = ld.database AND l.id = ld.learn_id
				WHERE lu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3000 HOUR)
) AS S

ON (T.database = S.database AND T.learn_id = S.learn_id AND T.learnclass_id = S.learnclass_id AND T.user_id = S.user_id AND T.team_id > 0)
WHEN MATCHED  AND T.dateload < S.dateload THEN
  UPDATE SET 
					learn_id = S.learn_id,
					learnclass_id = S.learnclass_id,
					user_id = S.user_id,
					drop_name = S.drop_name,
					learn_description = S.learn_description,
					status = S.status,
					team_id = S.team_id,
					classname = S.classname,
					class_description = S.class_description,
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
					user_id,
					drop_name,
					learn_description,
					status,
					team_id,
					classname,
					class_description,
					by_groups,
					average_time_finish,
					selective_process,
					publishin_date,
					finishin_date,
					class_status,
					class_type,					
                    database,
                    dateload,
					op,
					sk_learnclass,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
  					S.learn_id,
					S.learnclass_id,
					S.user_id,
					S.drop_name,
					S.learn_description,
					S.status,
					S.team_id,
					S.classname,
					S.class_description,
					S.by_groups,
					S.average_time_finish,
					S.selective_process,
					S.publishin_date,
					S.finishin_date,
					S.class_status,
					S.class_type,	
                    S.database,
                    S.dateload,
					S.op,
					S.sk_learnclass,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )	
	
	
	
	
	
	
	

/*

# questions -----------------------------------------VINHA DA LEARNDETAILL ALL >>>> FAZER JOIN COM LEARNDROP E DROP leanrCOMQUESTINOS PARA SABER QUANTAS QUESTÕES

SELECT 
  l.id AS learn_id,
  COUNT(DISTINCT lq.title) AS questions,
  sum(weight) AS answers,
FROM beegdata_dev_trusted.learn l
INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
INNER JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
INNER JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id #AND la.weight IN(1)
#WHERE l.id = 55747
GROUP BY l.id

*/

