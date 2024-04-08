MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
		SELECT
				IFNULL(q.learn_id, -1) AS learn_id,
				IFNULL(q.learnclass_id, -1) AS learnclass_id,
				IFNULL(q.user_id, -1) AS user_id,
				q.team_id,
				q.goal_type,
				q.questions,
				q.answers,
				q.correct_answers,
				q.assertiveness,
				q.score_obtained,
				q.points,
				q.goal,
				q.result,
				q.response_date,
				q.attempts,
				q.attempt,
				q.database, 
				q.dateload,
				CASE
						WHEN q.attempt = 1 AND q.result = "Não entregue" THEN '+1'
					  ELSE CAST(q.aremaining_treis AS STRING)
					END remaining_treis,
				
				CASE 
						WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 0
						ELSE q.attempt
					END user_attempts,
			  
				CASE 
						WHEN q.attempt = 0 AND q.result = "Não entregue" THEN 'Ainda não entregou'
						WHEN q.attempt > 0 AND q.result = "Não entregue" THEN 'Ele já reprovou pelo menos 1 vez'
						WHEN q.attempt > 0 AND 
								 CASE WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 1 ELSE q.aremaining_treis END > 0 
								 AND q.result = "Reprovado" THEN 'Ele já reprovou pelo menos 1 vez'
						WHEN q.attempt > 0 AND
							   CASE WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 1 ELSE q.aremaining_treis END = 0
								 AND q.result = "Reprovado" THEN 'Ele errou todas as tentativas'
						WHEN q.attempt > 0 AND q.result = "Aprovado" THEN 'Ele acertou em alguma das tentativas'
						ELSE CAST(aremaining_treis AS STRING)
					END result_attempts,
					dl.sk_learnclass,
					du.sk_user,
					di.sk_instructor,
					CAST(CONCAT(q.learnclass_id, q.user_id, CASE WHEN q.database = 'ambev' THEN 1 WHEN q.database = "beedoo" THEN 2 WHEN q.database = "fis" THEN 3 WHEN q.database = "jv" THEN 4 WHEN q.database = "mapfre" THEN 5 ELSE -1 END) AS BIGNUMERIC) AS sk_students,
					q.learntype_id AS sk_category
		FROM (
					SELECT
							l.id AS learn_id,
							lc.id AS learnclass_id,
							lu.user_id,
							l.team_id,
							l.learntype_id,
							CASE 
								WHEN l.learn_goal_type_id = 1 THEN "qtd_perguntas"
								WHEN l.learn_goal_type_id = 2 THEN "qtd_beetcoins"
							END goal_type,
							COUNT(DISTINCT lq.title) AS questions,
							COUNT(la.title) AS answers,
							COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) AS correct_answers,
							
							CASE
									WHEN l.learn_goal_type_id = 1 THEN CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / NULLIF(COUNT(la.title),0) * 100 AS INTEGER) --GOAL 1
									WHEN l.learn_goal_type_id = 2 THEN CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / NULLIF(sum(lq.points),0) * 100 AS INTEGER) --"GOAL 2"
								END assertiveness,
							SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) AS score_obtained,
							SUM(lq.points) AS points,
							l.meta as goal,
							
							CASE
									WHEN lu.id IS NULL THEN "Não ingressado"
									WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "Reprovado" 
									WHEN DATE_DIFF(lc.finishin, CURRENT_TIMESTAMP(), HOUR) < 0 AND lu.response_date IS NULL THEN "Não entregue" -- < TIMESTAMP("1970-01-01")
									WHEN l.learn_goal_type_id = 1  AND lu.response_date > TIMESTAMP("1900-01-01") AND 
												CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id 
														AND la.weight = 1 THEN la.id END) / NULLIF(COUNT(la.title),0) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
									WHEN l.learn_goal_type_id = 2 AND lu.response_date > TIMESTAMP("1900-01-01") AND 
												CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / NULLIF(sum(lq.points),0) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
									ELSE "Reprovado"
								END AS result,
							lc.instructor_id,
							--lc.by_groups,
							lc.finishin, 
							lu.response_date,
							lc.attempts,
							lu.attempt,
							lc.attempts - lu.attempt AS aremaining_treis, --total de tentativas total feitas e ainda pra fazer
							l.database,
							lu.dateload
					FROM beegdata_dev_trusted.learn l
					LEFT JOIN beegdata_dev_trusted.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
					--LEFT JOIN beegdata_dev_trusted.learnclassuser lu ON lc.database = lu.database AND lc.id = lu.learnclass_id #######
					LEFT JOIN (
										SELECT database, id, learnclass_id, user_id, attempt, response_date, dateload,
													 ROW_NUMBER() OVER (PARTITION BY database, learnclass_id, user_id ORDER BY id DESC) AS rn
										FROM beegdata_dev_trusted.learnclassuser
						) AS lu ON lc.database = lu.database AND lc.id = lu.learnclass_id AND lu.rn = 1
					LEFT JOIN beegdata_dev_trusted.learndrop ld ON l.database = ld.database AND l.id = ld.learn_id
					LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.database = lq.database AND ld.id = lq.learndrop_id
					LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.database = la.database AND lq.id = la.learnquestion_id AND la.weight IN(1)
					LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.database = lr.database AND la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
					INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.database = lg.database AND l.learn_goal_type_id =  lg.id
					GROUP BY l.id, lc.id, lu.user_id, l.team_id, l.learntype_id, lc.finishin, l.database, lu.id, lu.response_date, l.meta, l.learn_goal_type_id, lc.instructor_id, lc.attempts, lu.attempt, lc.finishin, lu.response_date, lu.dateload
				) AS q
			
		LEFT JOIN {dataset_refined}.dim_user AS du ON q.database = du.database AND q.user_id  = du.user_id AND du.scd_activate = 1  and du.team_id > 0
		LEFT JOIN {dataset_refined}.dim_learnclass AS dl ON q.database = dl.database AND q.learn_id = dl.learn_id AND q.learnclass_id = dl.learnclass_id AND dl.scd_activate = 1  and dl.team_id > 0
		LEFT JOIN {dataset_refined}.dim_instructor AS di ON q.database = di.database AND q.instructor_id  = di.instructor_id AND di.scd_activate = 1
		LEFT JOIN {dataset_refined}.dim_groupuser AS dg ON q.database = dg.database AND q.user_id = dg.user_id AND dg.rnk = 1 AND dg.team_id > 0
						
) AS S
ON (T.database = S.database AND T.learn_id = S.learn_id AND T.learnclass_id = S.learnclass_id AND T.user_id = S.user_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
					goal_type = S.goal_type,
					questions = S.questions,
					answers = S.answers,
					correct_answers = S.correct_answers,
					assertiveness = S.assertiveness,
					score_obtained = S.score_obtained,
					points = S.points,
					goal = S.goal,
					result = S.result,
					response_date = S.response_date,
					attempts = S.attempts,
					attempt = S.attempt,
					remaining_treis = S.remaining_treis,
					result_attempts = S.result_attempts,
                    database = S.database
WHEN NOT MATCHED THEN
  INSERT (
					learn_id,
					learnclass_id,
					user_id,
					team_id,
					goal_type,
					questions,
					answers,
					correct_answers,
					assertiveness,
					score_obtained,
					points,
					goal,
					result,
					response_date,
					attempts,
					attempt,
					remaining_treis,
					result_attempts,
					database,
                    dateload,
					sk_learnclass,
					sk_user,
					sk_instructor,
					sk_students,
					sk_category
                    )
  VALUES (
					S.learn_id,
					S.learnclass_id,
					S.user_id,
					S.team_id,
					S.goal_type,
					S.questions,
					S.answers,
					S.correct_answers,
					S.assertiveness,
					S.score_obtained,
					S.points,
					S.goal,
					S.result,
					S.response_date,
					S.attempts,
					S.attempt,
					S.remaining_treis,
					S.result_attempts,
					S.database, 
                    S.dateload,
					S.sk_learnclass,
					S.sk_user,
					S.sk_instructor,
					S.sk_students,
					S.sk_category
                    )
					
					



						
							
							
							
							
							
							
/*

select * from `focus-mechanic-321819.beegdata_dev_refined.dim_students` ds
--left join `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fl on fl.sk_students = ds.sk_students
where ds.team_id > 0 --and fl.team_id < 0
and ds.database = 'ambev'
and ds.learnclass_id = 164611
---- esta query bate, o problema é que team_id da fato não pode ter restrição



SELECT --onde ATTMPT É ZERO então vem do grupo \ tem 2.725.882 de linhas com attempt igua a 0 \ tem 1.037.513 com attempt maior que 0 vindo de user mesmo valor que userclass
    sq.database, sq.learnclass_id, sq.user_id, sq.attempt, sq.response_date, sq.dateload,
    ROW_NUMBER() OVER (PARTITION BY database, learnclass_id, user_id ORDER BY attempt DESC) AS rn
FROM ( --1,3 GB
        SELECT DISTINCT
        lp.database, lp.learnclass_id, gu.user_id, 0 attempt,   CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS response_date, CAST('1900-01-01 00:00:00 UTC' AS TIMESTAMP) AS dateload,
        FROM beegdata_dev_trusted.learnclassgroup lp 
        LEFT JOIN beegdata_dev_trusted.groupuser gu ON gu.group_id = lp.group_id AND gu.database = lp.database 

        UNION DISTINCT --1,05 GB

        SELECT DISTINCT
        lu.database, lu.learnclass_id, lu.user_id, attempt, response_date, dateload,
        FROM beegdata_dev_trusted.learnclassuser AS lu
) AS sq where attempt > 0
*/
