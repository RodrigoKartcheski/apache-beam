MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
 		    SELECT
					mh.id AS idmood,
					mh.user_id,
					mh.team_id,
					mh.message, 
					mh.created_at, 
					m.title, 
					m.status,
					u.score,
					u.firstaccess,
                    u.lastaccess,
					mh.database, 
                    CAST(mh.dateload AS TIMESTAMP) AS dateload,
					mh.op,
					du.sk_user
			FROM {dataset_trusted}.mood_history mh 
			INNER JOIN {dataset_trusted}.mood m ON mh.database = m.database AND mh.mood_id = m.id
			INNER JOIN {dataset_trusted}.user AS u ON mh.database = u.database AND mh.user_id  = u.id
			INNER JOIN {dataset_refined}.dim_user AS du ON mh.database = du.database AND mh.user_id  = du.user_id AND du.scd_activate = 1 
				WHERE mh.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND du.team_id > 0
) AS S
ON (T.database = S.database AND T.user_id = S.user_id AND T.idmood = S.idmood AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
                    user_id = S.user_id,
					team_id = S.team_id,
                    message = S.message,
                    created_at = S.created_at,
                    title = S.title,
                    status = S.status,
                    score = S.score,
					firstaccess = S.firstaccess,
                    lastaccess = S.lastaccess,
                    database = S.database,
					op = S.op
WHEN NOT MATCHED THEN
  INSERT (
					idmood,
                    user_id,
					team_id,
                    message,
                    created_at,
                    title,
                    status,
                    score,
					firstaccess,
                    lastaccess,
                    database,
                    dateload,
					op,
					sk_user
                    )
  VALUES (  
					S.idmood,
                    S.user_id,
					S.team_id,
                    S.message,
                    S.created_at,
                    S.title,
                    S.status,
                    S.score,
					S.firstaccess,
                    S.lastaccess,
                    S.database,
                    S.dateload,
					S.op,
					S.sk_user
                    )
