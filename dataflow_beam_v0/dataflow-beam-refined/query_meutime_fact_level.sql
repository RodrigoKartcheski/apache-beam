MERGE INTO beegdata_dev_refined.fact_level AS T
USING (
			SELECT * FROM (
					SELECT 
						u.id as user_id, l.team_id, l.name as level_name, u.score as user_score, u.database, u.dateload
					   ,row_number() over (partition by u.id, u.team_id order by score_next_level desc) as rnb
					FROM (
								SELECT id, team_id, name, database, COALESCE(score, 0) as score, COALESCE(LEAD(score) OVER (PARTITION BY team_id ORDER BY score),0) as score_next_level
								FROM beegdata_dev_trusted.level
									WHERE team_id <> 0
					) l 
					INNER JOIN beegdata_dev_trusted.user u ON u.team_id = l.team_id
					 AND u.score BETWEEN l.score AND l.score_next_level
					 AND u.database = l.database
			  ) AS temp_rnb 
			  WHERE rnb = 1
) AS S
ON (T.database = S.database AND T.user_id = S.user_id)
WHEN MATCHED THEN
  UPDATE SET
					user_id = S.user_id,
					team_id = S.team_id,
					level_name = S.level_name,
					user_score = S.user_score,
					database = s.database,
					dateload = S.dateload
WHEN NOT MATCHED THEN
  INSERT (
					user_id, 
					team_id, 
					level_name, 
					user_score,
					database,
					dateload
                    )
  VALUES (  
					S.user_id, 
					S.team_id, 
					S.level_name, 
					S.user_score,
					S.database,
					S.dateload
                    )