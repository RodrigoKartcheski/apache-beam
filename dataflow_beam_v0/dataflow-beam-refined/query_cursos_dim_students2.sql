MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
SELECT  --distinct é user_id + learnclass_id + database \\ trazendo a grupos o distinct  é user_id + learnclass_id + group_id + database 
			q.user_id,
			q.learn_id,
			q.learnclass_id,
			q.by_groups,
			q.team_id,
			ARRAY_AGG(STRUCT(gu.group_id, g.name AS group_name)) AS groupuser,
			--ARRAY_AGG(gu.group_id) AS group_id,
			--ARRAY_AGG(CAST(gu.group_id AS STRING) IGNORE NULLS) as group_id,
			--'1' as group_id,
			--ARRAY_AGG(STRUCT(g.name)) AS group_name,
			q.database,
			CAST(CONCAT(q.learnclass_id, q.user_id, CASE WHEN q.database = 'ambev' THEN 1 WHEN q.database = "beedoo" THEN 2 WHEN q.database = "fis" THEN 3 WHEN q.database = "jv" THEN 4 WHEN q.database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_students
FROM (
			  SELECT DISTINCT 
						gu.user_id, l.id as learn_id, lg.learnclass_id, lc.by_groups, 2 team_id, lg.database
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg
			  INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` AS gu ON gu.group_id = lg.group_id AND gu.database = lg.database
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lg.learnclass_id AND lc.database = lg.database
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learn` AS l ON l.id = lc.learn_id AND l.database = lc.database
				WHERE lg.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND lc.by_groups = 1 --; --4.512.418 total de linhas E 2.725.681 USER DISTINCT MATRICULADOS
				AND lg.op <> 'DELETE' 
			  
			  UNION ALL   -- 2.793.398 linhas distinstas somente usuarios

			  SELECT DISTINCT 
						lu.user_id, l.id as learn_id, lu.learnclass_id, lc.by_groups, 2 team_id, lu.database
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassuser` AS lu
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lu.learnclass_id AND lc.database = lu.database
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learn` AS l ON l.id = lc.learn_id AND l.database = lc.database
				WHERE lu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND by_groups = 0 --67.772 total de linhas E 67.171 unicos
			) AS q
LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` gu ON gu.user_id = q.user_id AND gu.database = q.database
LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.group` g ON g.id = gu.group_id
	GROUP BY
	q.user_id, q.learn_id, q.learnclass_id, q.by_groups, q.team_id, q.database
) AS S

ON (T.database = S.database AND T.learnclass_id = S.learnclass_id AND T.user_id = S.user_id AND T.team_id > 0)
/*WHEN MATCHED  AND T.dateload < S.dateload THEN
  UPDATE SET
					user_id = S.user_id,
					learnclass_id = S.learnclass_id,
					by_groups = S.by_groups
					team_id = S.team_id,
					database = S.database, 
					scd_finished = CURRENT_TIMESTAMP(),
					scd_activate = 0*/
WHEN NOT MATCHED THEN
  INSERT (
					user_id,
					learn_id,
					learnclass_id,
					by_groups,
					team_id,
					groupuser,
                    database,
					sk_students,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
					S.user_id,
					S.learn_id,
					S.learnclass_id,
					S.by_groups,
					S.team_id,
					S.groupuser,
                    S.database,
					S.sk_students,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )