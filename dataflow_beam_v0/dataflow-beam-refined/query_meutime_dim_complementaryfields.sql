MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT DISTINCT ############### APOS A CARGA FULL, ANALISAR SE A TABELA userfield ESTA DUPLICANDO DADOS DA TRUSTED
				  mu.user_id
				, u.team_id as team_id
				, mu.customfield_id
				, mc.name as customfield_name
				, mc.type as customfield_type
				, COALESCE(mu.value, ml.value) as customfield_value
				,REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(mc.name),'Á','A'), '[^a-zA-Z0-9_ ]', ''), ' ', '_') as customfield_name_clean
				,mu.cdc_commit_date
				,mu.database
				,mu.dateload
				,CAST(CONCAT(mu.user_id, mu.customfield_id, mu.user_id, CASE WHEN mu.database = 'ambev' THEN 1 WHEN mu.database = "beedoo" THEN 2 WHEN mu.database = "fis" THEN 3 WHEN mu.database = "jv" THEN 4 WHEN mu.database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_complementaryfields
			FROM beegdata_dev_trusted.userfield AS mu
			LEFT JOIN beegdata_dev_trusted.user AS u ON u.id = mu.user_id AND u.database = mu.database
			LEFT JOIN beegdata_dev_trusted.customfieldlist AS ml ON ml.id = mu.customfieldlist_id AND ml.database = mu.database
			INNER JOIN beegdata_dev_trusted.customfield AS mc ON mc.id = mu.customfield_id AND mc.database = mu.database
                WHERE mu.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
) AS S

ON (T.database = S.database AND T.user_id = S.user_id AND T.customfield_id = S.customfield_id AND T.team_id > 0)
WHEN MATCHED  AND T.dateload < S.dateload THEN
  UPDATE SET 
				    user_id = S.user_id,
				    team_id = S.team_id,
				    customfield_id = S.customfield_id,
				    customfield_name = S.customfield_name,
				    customfield_type = S.customfield_type,
				    customfield_value = S.customfield_value,
				    customfield_name_clean = S.customfield_name_clean,
				    cdc_commit_date = S.cdc_commit_date,
                    database = S.database,
					scd_finished = CURRENT_TIMESTAMP(),
					scd_activate = 0
WHEN NOT MATCHED THEN
  INSERT (
				    user_id,
				    team_id,
				    customfield_id,
				    customfield_name,
				    customfield_type,
				    customfield_value,
				    customfield_name_clean,
				    cdc_commit_date,
                    database,
					dateload,
					sk_complementaryfields,
					scd_started,
					scd_finished,
					scd_activate
                    )
  VALUES (  
				    S.user_id,
				    S.team_id,
				    S.customfield_id,
				    S.customfield_name,
				    S.customfield_type,
				    S.customfield_value,
				    S.customfield_name_clean,
				    S.cdc_commit_date,
                    S.database,
                    S.dateload,
					S.sk_complementaryfields,
					CURRENT_TIMESTAMP(),
					'1900-01-01 00:00:00',
					1
                    )