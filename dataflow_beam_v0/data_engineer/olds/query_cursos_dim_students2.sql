MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
SELECT  --distinct é user_id + learnclass_id + database \\ trazendo a grupos o distinct  é user_id + learnclass_id + group_id + database 
			q.user_id,
			ARRAY_AGG(STRUCT(gu.group_id, q.group_name)) AS groupuser,
			q.learnclass_id,
			q.by_groups,
			q.team_id,
			q.database,
			CAST(CONCAT(learnclass_id, user_id, CASE WHEN q.database = 'ambev' THEN 1 WHEN database = "beedoo" THEN 2 WHEN database = "fis" THEN 3 WHEN database = "jv" THEN 4 WHEN database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_students THEN 2 ELSE -1 END) AS BIGNUMERIC) AS sk_students
FROM (
			  SELECT DISTINCT 
						gu.user_id, lg.learnclass_id, lc.by_groups, 2 team_id, lg.database
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg
			  INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` AS gu ON gu.group_id = lg.group_id AND gu.database = lg.database
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lg.learnclass_id AND lc.database = lg.database
				WHERE lg.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND lc.by_groups = 1 --; --4.512.418 total de linhas E 2.725.681 USER DISTINCT MATRICULADOS
				AND lg.op <> 'DELETE' 
			  
			  UNION ALL   -- 2.793.398 linhas distinstas somente usuarios

			  SELECT DISTINCT 
						lu.user_id, lu.learnclass_id, lc.by_groups, 2 team_id, lu.database
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassuser` AS lu
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lu.learnclass_id AND lc.database = lu.database
				WHERE lu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND by_groups = 0 --67.772 total de linhas E 67.171 unicos
			) AS q
LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` gu ON gu.user_id = q.user_id -- 718,02 MB --13.694.980 linhas
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
					S.learnclass_id,
					S.by_groups,
					S.team_id,
					s.groupuser,
                    S.database,
					S.sk_students,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )
					













####################################################
DIM ALUNOS 
####################################################
SELECT
    user_id, 
    learnclass_id,
    by_groups,
    database
FROM (
    SELECT DISTINCT 
        gu.user_id, lg.learnclass_id, lc.by_groups, lg.database
    FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg
    INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` AS gu ON gu.group_id = lg.group_id AND gu.database = lg.database
    LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lg.learnclass_id AND lc.database = lg.database
      WHERE lg.op <> 'DELETE' 
        AND by_groups = 1 --; --4.512.418 total de linhas E 2.725.681 USER DISTINCT MATRICULADOS
    
    UNION ALL   -- 2.793.398 linhas distinstas somente usuarios

    SELECT DISTINCT 
        lu.user_id, lu.learnclass_id, lc.by_groups, lu.database
    FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassuser` AS lu
    LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lu.learnclass_id AND lc.database = lu.database
      WHERE by_groups = 0 --; --67.772 total de linhas E 67.171 unicos
) AS query














''''
MERGE INTO beegdata_dev_refined.dim_students2 AS T
USING (
SELECT  --distinct é user_id + learnclass_id + database \\ trazendo a grupos o distinct  é user_id + learnclass_id + group_id + database 
			q.user_id,
			--ARRAY_AGG(STRUCT(gu.group_id, q.group_name))[OFFSET(0)] AS groupuser,
			ARRAY_AGG(STRUCT(gu.group_id, q.group_name)) AS groupuser,
			--ARRAY_AGG(STRUCT(author_id, author_name, date_of_birth)) AS authors
			--ARRAY[
       --    STRUCT(gu.group_id, q.group_name)
        -- ] AS addresses,
			q.learnclass_id,
			q.by_groups,
      q.team_id,
			q.database,
			CAST(CONCAT(q.learnclass_id, q.user_id, CASE WHEN q.database = 'ambev' THEN 1 WHEN q.database = "beedoo" THEN 2 WHEN q.database = "fis" THEN 3 WHEN q.database = "jv" THEN 4 WHEN q.database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_students
FROM (
			  SELECT DISTINCT 
						gu.user_id, lg.learnclass_id, lc.by_groups, 2 team_id, lg.database, "a" AS group_name
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg
			  INNER JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` AS gu ON gu.group_id = lg.group_id AND gu.database = lg.database
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lg.learnclass_id AND lc.database = lg.database
				WHERE lg.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3000 HOUR)
				AND lc.by_groups = 1 --; --4.512.418 total de linhas E 2.725.681 USER DISTINCT MATRICULADOS
				AND lg.op <> 'DELETE' 
			  
			  UNION ALL   -- 2.793.398 linhas distinstas somente usuarios

			  SELECT DISTINCT 
						lu.user_id, lu.learnclass_id, lc.by_groups, 2 team_id, lu.database, "A" as group_name
			  FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassuser` AS lu
			  LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclass` AS lc ON lc.id = lu.learnclass_id AND lc.database = lu.database
				WHERE lu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3000 HOUR)
				AND by_groups = 0 --67.772 total de linhas E 67.171 unicos
			) AS q
			LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.groupuser` gu ON gu.user_id = q.user_id -- 718,02 MB --13.694.980 linhas
			 GROUP BY 			
			q.user_id,
			q.learnclass_id,
			q.by_groups,
      q.team_id,
			q.database,
			sk_students
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
					learnclass_id,
					by_groups,
					team_id,
					groupuser,
          database,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
					S.user_id,
					S.learnclass_id,
					S.by_groups,
					S.team_id,
					S.groupuser,
          S.database,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )