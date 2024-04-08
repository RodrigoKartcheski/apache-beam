MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT
					IFNULL(q.learnquestions_id, -1) AS learnquestions_id,
					IFNULL(q.learn_id, -1) AS learn_id,
					IFNULL(q.learnclass_id, -1) AS learnclass_id,
					IFNULL(q.user_id, -1) AS user_id,
					q.team_id,
					q.instructor_id,
					q.by_groups,
					q.goal,
					q.goal_type,
					q.question,
					q.response,
					q.correct_answers,
					q.difficulty,
					q.response_date,
					q.database,
					q.dateload,
					q.op,
					dl.sk_learnclass,
					du.sk_user,
					di.sk_instructor,
					CAST(CONCAT(q.learnclass_id, q.user_id, CASE WHEN q.database = 'ambev' THEN 1 WHEN q.database = "beedoo" THEN 2 WHEN q.database = "fis" THEN 3 WHEN q.database = "jv" THEN 4 WHEN q.database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_students,
					q.learntype_id AS sk_category
			FROM
						(
						SELECT DISTINCT
								---	CAST(CONCAT(lu.id,la.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(la.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(la.dateload AS DATE), la.id)) AS  BIGNUMERIC) AS sk_learnquestions,
								la.id AS learnquestions_id,
								l.id AS learn_id,
								lc.id AS learnclass_id,
								lu.user_id,
								l.team_id,
								l.learntype_id,
								lc.instructor_id,
								lc.by_groups,
								l.meta AS goal,
								lg.code AS goal_type,
								lq.title AS question,
								la.title AS response,
								CASE
										WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN "Acertou"
										WHEN la.id = lr.learnanswer_id AND la.weight = 0 THEN "Errou"
										ELSE ""
									END AS correct_answers,
								CASE
										WHEN lq.learn_questions_bank_difficulty_id = -1 THEN 'nao definido'
										WHEN lq.learn_questions_bank_difficulty_id = 1 THEN 'facil'
										WHEN lq.learn_questions_bank_difficulty_id = 2 THEN 'media'
										WHEN lq.learn_questions_bank_difficulty_id = 3 THEN 'dificil'
									END difficulty,
								lu.response_date,
								ARRAY_AGG(STRUCT(lcg.learnclass_id)) AS groupuser,
								l.database, 
								CAST(la.dateload AS TIMESTAMP) AS dateload,
								lq.op,
								--dl.sk_learnclass,
								--du.sk_user,
								--di.sk_instructor,
								--dg.sk_groupuser,
								ROW_NUMBER() OVER (PARTITION BY l.id, lc.id, lu.user_id, lq.title, la.title ORDER BY la.id) AS rn
						FROM {dataset_trusted}.learn l
						LEFT JOIN {dataset_trusted}.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
						LEFT JOIN {dataset_trusted}.learnclassgroup lcg ON lcg.database = lc.database AND lcg.learnclass_id = lc.id
						--LEFT JOIN {dataset_trusted}.learnclassuser lu ON lc.database = lu.database AND  lc.id = lu.learnclass_id #######
						LEFT JOIN (
										SELECT database, id, learnclass_id, user_id, attempt, response_date, dateload,
													 ROW_NUMBER() OVER (PARTITION BY database, learnclass_id, user_id ORDER BY id DESC) AS rn
										FROM beegdata_dev_trusted.learnclassuser
						) AS lu ON lc.database = lu.database AND lc.id = lu.learnclass_id AND lu.rn = 1
						LEFT JOIN {dataset_trusted}.learndrop ld ON l.database = ld.database AND l.id = ld.learn_id
						LEFT JOIN {dataset_trusted}.learnquestion lq ON ld.database = lq.database AND  ld.id = lq.learndrop_id
						LEFT JOIN {dataset_trusted}.learnanswer la ON lq.database = la.database AND  lq.id = la.learnquestion_id --AND la.weight IN(1)
						LEFT JOIN  {dataset_trusted}.learnansweruser lr ON la.database = lr.database AND  la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
						INNER JOIN {dataset_trusted}.learn_goal_type lg ON l.database = lg.database AND  l.learn_goal_type_id =  lg.id
						--LEFT JOIN {dataset_refined}.dim_user AS du ON l.database = du.database AND lu.user_id  = du.user_id AND du.scd_activate = 1  and du.team_id > 0
						--LEFT JOIN {dataset_refined}.dim_learnclass AS dl ON l.database = dl.database AND l.id = dl.learn_id AND lc.id = dl.learnclass_id AND lu.user_id  = dl.user_id AND dl.scd_activate = 1  and dl.team_id > 0
						--LEFT JOIN {dataset_refined}.dim_instructor AS di ON l.database = di.database AND lc.instructor_id  = di.instructor_id AND di.scd_activate = 1
						--LEFT JOIN {dataset_refined}.dim_groupuser AS dg ON l.database = dg.database AND lu.user_id = dg.user_id AND dg.rnk = 1 AND dg.team_id > 0
							WHERE la.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
							--AND du.team_id > 0
						) AS q 
			LEFT JOIN {dataset_refined}.dim_user AS du ON q.database = du.database AND q.user_id  = du.user_id AND du.scd_activate = 1  and du.team_id > 0
			LEFT JOIN {dataset_refined}.dim_learnclass AS dl ON q.database = dl.database AND q.learn_id = dl.learn_id AND q.learnclass_id = dl.learnclass_id AND dl.scd_activate = 1  and dl.team_id > 0
			LEFT JOIN {dataset_refined}.dim_instructor AS di ON q.database = di.database AND q.instructor_id  = di.instructor_id AND di.scd_activate = 1
			--LEFT JOIN {dataset_refined}.dim_groupuser AS dg ON q.database = dg.database AND q.user_id = dg.user_id AND dg.rnk = 1 AND dg.team_id > 0
				WHERE q.rn = 1
			
						
						
						
) AS S
ON (T.database = S.database AND T.learn_id = S.learn_id AND T.learnclass_id = S.learnclass_id AND T.user_id = S.user_id AND T.learnquestions_id = s.learnquestions_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
					by_groups = S.by_groups, 
					question = S.question,
					response = S.response,
					correct_answers = S.correct_answers,
					difficulty = S.difficulty,
					response_date = S.response_date,
                    database = S.database,
					op = S.op
WHEN NOT MATCHED THEN
  INSERT (
					learnquestions_id,
					learn_id,
					learnclass_id,
					user_id,
					team_id,
					by_groups,
					question,
					response,
					correct_answers,
					difficulty,
					response_date,
					database, 
                    dateload,
					op,
					sk_learnclass,
					sk_user,
					sk_instructor,
					sk_students,
					sk_category
					
                    )
  VALUES (
					S.learnquestions_id,
					S.learn_id,
					S.learnclass_id,
					S.user_id,
					S.team_id,
					S.by_groups,
					S.question,
					S.response,
					S.correct_answers,
					S.difficulty,
					S.response_date,
					S.database, 
                    S.dateload,
					S.op,
					S.sk_learnclass,
					S.sk_user,
					S.sk_instructor,
					S.sk_students,
					sk_category
                    )
					
					

