MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
 		    SELECT 
					ua.id AS access_id,
					ua.user_id,
					du.team_id,
					ua.datetime AS dateaccess,
					ua.database, 
                    CAST(ua.dateload AS TIMESTAMP) AS dateload,
					ua.op,
					du.sk_user,
					0 sk_groupuser
					--dgu.sk_groupuser
			FROM {dataset_trusted}.useraccess ua 
			INNER JOIN {dataset_refined}.dim_user AS du ON ua.database = du.database AND ua.user_id  = du.user_id AND du.scd_activate = 1 
			--LEFT JOIN {dataset_refined}.dim_groupuser AS dgu ON ua.database = dgu.database AND ua.user_id  = dgu.user_id AND dgu.team_id > 0 AND dgu.rnk = 1 AND dgu.scd_activate = 1 
				WHERE ua.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND du.team_id > 0
) AS S
ON (T.database = S.database AND T.user_id = S.user_id AND T.access_id = S.access_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
 					access_id = S.access_id,
					user_id = S.user_id,
					team_id = S.team_id,
					dateaccess = S.dateaccess,
                    database = S.database,
					op = S.op
WHEN NOT MATCHED THEN
  INSERT (
					access_id,
					user_id,
					team_id,
					dateaccess,
					database, 
                    dateload,
					op,
					sk_user,
					sk_groupuser
                    )
  VALUES (  
					S.access_id,
					S.user_id,
					S.team_id,
					S.dateaccess,
					S.database, 
                    S.dateload,
					S.op,
					S.sk_user,
					S.sk_groupuser
                    )
