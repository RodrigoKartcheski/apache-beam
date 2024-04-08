MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT DISTINCT
				lt.id AS category_id, 
				lt.title AS category, 
				lt.team_id, 
				lt.user_id, 
				lt.database,
				lt.dateload,
				lt.id AS sk_category
			FROM `focus-mechanic-321819.beegdata_dev_trusted.learntype` AS lt
) AS S

ON (T.database = S.database AND T.category_id = S.category_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
			        category_id = S.category_id, 
				    category = S.category, 
				    team_id = S.team_id, 
				    user_id = S.user_id, 
				    database = S.database
					--scd_finished = CURRENT_TIMESTAMP(),
					--scd_activate = 0
WHEN NOT MATCHED THEN
  INSERT (
			        category_id, 
				    category, 
				    team_id, 
				    user_id, 
				    database,
					sk_category,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
			        S.category_id, 
				    S.category, 
				    S.team_id, 
				    S.user_id, 
				    S.database,
					S.sk_category,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )