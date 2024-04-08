MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT DISTINCT
					gu.group_id,
					--g.name AS group_name,
					gu.user_id,
					u.name As user_name,
					u.team_id,
					gu.database, 
					CAST(gu.dateload AS TIMESTAMP) AS dateload,
					gu.op,
					ROW_NUMBER() OVER(PARTITION BY gu.user_id ORDER BY gu.dateload DESC) AS rnk,
					CAST(CONCAT(gu.user_id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(gu.dateload AS DATE)))) AS BIGNUMERIC) AS sk_groupuser
			FROM  `beegdata_dev_trusted.user` AS u
			INNER JOIN `beegdata_dev_trusted.groupuser` AS gu ON u.database = gu.database AND u.id = gu.user_id
			--INNER JOIN `beegdata_dev_trusted.group` AS g ON gu.database = g.database AND gu.group_id = g.id
                WHERE gu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
) AS S

ON (T.database = S.database AND T.group_id = S.group_id AND T.user_id = S.user_id AND T.team_id > 0)
WHEN MATCHED  AND T.dateload < S.dateload THEN
  UPDATE SET 
					rnk = S.rnk,
					group_id = S.group_id,
					--group_name = S.group_name,
					user_id = S.user_id,
					user_name = S.user_name,
					team_id = S.team_id,
					database = S.database, 
					dateload = S.dateload,
					op = S.op,
					scd_finished = CURRENT_TIMESTAMP(),
					scd_activate = 0
WHEN NOT MATCHED THEN
  INSERT (
					rnk,
					sk_groupuser,
					group_id,
					--group_name,
					user_id,
					user_name,
					team_id,
                    database,
                    dateload,
					op,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
					S.rnk,
					S.sk_groupuser,
					S.group_id,
					--S.group_name,
					S.user_id,
					S.user_name,
					S.team_id,
                    S.database,
                    S.dateload,
					S.op,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )
					
		
		
/*
CREATE OR REPLACE TABLE  `focus-mechanic-321819.beegdata_dev_refined.dim_groupuser2` 
AS
SELECT 
    q.group_id, q.user_id, q.learnclass_id,q.database,
    ROW_NUMBER() OVER(PARTITION BY q.learnclass_id, q.user_id, q.database) AS rnk,
    CAST(CONCAT(q.user_id, CASE WHEN q.database = 'ambev' then 1 WHEN q.database = 'beedoo' then 2 END) AS BIGNUMERIC) AS sk_groupuser
FROM (
    SELECT DISTINCT
        dg.group_id, dg.user_id, lg.learnclass_id, dg.database
    FROM `focus-mechanic-321819.beegdata_dev_refined.dim_groupuser` AS dg
    LEFT JOIN `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg ON dg.database = lg.database AND dg.group_id = lg.group_id
    WHERE team_id > 0
) AS q;


INDICADOR PARTICIPANTE
    SELECT 
      count(distinct dg.user_id)
    FROM `focus-mechanic-321819.beegdata_dev_trusted.learnclassgroup` AS lg
    LEFT JOIN beegdata_dev_trusted.learnclass as lc ON lc.id = lg.learnclass_id
    LEFT JOIN `focus-mechanic-321819.beegdata_dev_refined.dim_groupuser` AS dg ON dg.database = lg.database AND dg.group_id = lg.group_id
    LEFT join `beegdata_dev_refined.dim_user` AS du ON du.user_id = dg.user_id
    WHERE du.team_id > 0 and dg.team_id > 0
    AND du.userstatus_id = 1
    and lc.by_groups = 1

    BY GROUP = 1 NÃO TRAS OS USUARIO NA TURMA O GRUPO E INSERIDO NA LEARNCLASSGROUP , BY GROUP 0 TRAS O USARIO E INSERIDO NA TURMA NSERIDO DIRETO NA LEARNCLASSUSER
	
	
	
	

SELECT count(du.user_id), result FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fl 
LEFT JOIN `focus-mechanic-321819.beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user
WHERE fl.team_id > 0
and  du.team_id > 0
GROUP BY result
-- com a dim user tirou os não ingressados



SELECT count(du.user_id), result FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fl 
LEFT JOIN `focus-mechanic-321819.beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user
WHERE fl.team_id > 0
and  du.team_id > 0
GROUP BY result
-- trazer o inicio e fim da turma


SELECT * FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` LIMIT 1000
--retornar o resonse date


*/
















/*


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

*/