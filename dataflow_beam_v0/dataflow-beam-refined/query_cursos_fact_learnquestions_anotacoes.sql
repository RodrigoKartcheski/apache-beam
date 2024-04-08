MERGE INTO {dataset_refined}.{destination_table} AS T
USING (
			SELECT DISTINCT
					---	CAST(CONCAT(lu.id,la.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(la.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(la.dateload AS DATE), la.id)) AS  BIGNUMERIC) AS sk_learnquestions,
					#la.id AS idlearnquestions,
					l.id AS learn_id,
					lc.id AS learnclass_id,
					lu.user_id,
					l.team_id,
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
					lq.learn_questions_bank_difficulty_id AS difficulty,
					lq.database, 
                    #CAST(lq.dateload AS TIMESTAMP) AS dateload,
					lq.op
					du.sk_user
			FROM {dataset_trusted}.learn l
			INNER JOIN {dataset_trusted}.learnclass lc ON l.id = lc.learn_id
			INNER JOIN {dataset_trusted}.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN {dataset_trusted}.learndrop ld ON l.id = ld.learn_id
			INNER JOIN {dataset_trusted}.learnquestion lq ON ld.id = lq.learndrop_id
			INNER JOIN {dataset_trusted}.learnanswer la ON lq.id = la.learnquestion_id AND la.weight IN(1)
			INNER JOIN {dataset_trusted}.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
			INNER JOIN {dataset_refined}.dim_user AS du ON l.database = du.database AND lu.user_id  = du.user_id AND du.scd_activate = 1 
				WHERE la.cdc_commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_time} HOUR)
				AND du.team_id > 0
) AS S
ON (T.database = S.database AND T.idlearnquestions = S.idlearnquestions AND T.learn_id = S.learn_id AND T.learnclass_id = S.learnclass_id AND T.user_id = S.user_id AND T.team_id > 0)
WHEN MATCHED THEN
  UPDATE SET
					by_groups = S.by_groups, 
					question = S.question,
					response = S.response,
					difficulty = S.difficulty,
                    database = S.database,
					op = S.op
WHEN NOT MATCHED THEN
  INSERT (
					idlearnquestions,
					learn_id,
					learnclass_id,
					user_id,
					team_id,
					by_groups,
					question,
					response,
					difficulty,
					database, 
                    dateload,
					op,
					sk_learnclass,
					sk_instructor_id,
					sk_user
                    )
  VALUES (
					S.idlearnquestions,
					S.learn_id,
					S.learnclass_id,
					S.user_id,
					S.team_id,
					S.by_groups,
					S.question,
					S.response,
					S.difficulty,
					S.database, 
                    S.dateload,
					S.op,
					1,
					1,
					sk_user
                    )
					
					
					
base de analise

			SELECT DISTINCT
					---	CAST(CONCAT(lu.id,la.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(la.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(la.dateload AS DATE), la.id)) AS  BIGNUMERIC) AS sk_learnquestions,
					#la.id AS idlearnquestions,
					l.id AS learn_id,
					lc.id AS learnclass_id,
					lu.user_id,
					l.team_id,
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
					lq.learn_questions_bank_difficulty_id AS difficulty,
					lq.database, 
                    #CAST(lq.dateload AS TIMESTAMP) AS dateload,
					lq.op
			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id #AND la.weight IN(1)
			LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
			WHERE l.id = 47607  
			--AND lc.id = 190106 
			AND lu.user_id = 690868
			
			
			
SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` 
WHERE TIMESTAMP_TRUNC(publishin_date, DAY) > TIMESTAMP("2000-06-27") 
AND learn_id = 47607 #and answers is not null AND learnclass_id = 190106 AND user_id = 841409
AND user_id = 690868



analise a abaixo com o Felipe
response_status
#################
			SELECT DISTINCT
					---	CAST(CONCAT(lu.id,la.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(la.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(la.dateload AS DATE), la.id)) AS  BIGNUMERIC) AS sk_learnquestions,
					la.id AS idlearnquestions,
					l.id AS learn_id,
					lc.id AS learnclass_id,
					lu.user_id,
					l.team_id,
					lc.instructor_id,
					lc.by_groups,
					l.meta AS goal,
					lg.code AS goal_type,
					lq.title AS question,
					la.title AS response,
					CASE
          	WHEN la.weight = 1 THEN 'Acertou'
          			ELSE "null"
      				END AS response_status, ############################################### regra de atemps
          lc.finishin,
          lu.response_date,
          CASE  
            WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "reprovado" 
              ELSE "APROVADO"
            END AS result,
          DATE_DIFF(lu.response_date, lc.finishin, DAY) AS date_diff,
					lq.learn_questions_bank_difficulty_id AS difficulty,
					lq.database, 
                    CAST(lq.dateload AS TIMESTAMP) AS dateload,
					lq.op,
					lq.id as dddd
			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			INNER JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			INNER JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id #AND la.weight IN(1)
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
			WHERE l.id = 47607  
			--AND lc.id = 190106 
			AND lu.user_id = 690868
		

####################################################################		
			
			SELECT DISTINCT
					---	CAST(CONCAT(lu.id,la.id, FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(CAST(la.dateload AS DATE))), ROW_NUMBER() OVER(ORDER BY CAST(la.dateload AS DATE), la.id)) AS  BIGNUMERIC) AS sk_learnquestions,
					#la.id AS idlearnquestions,
					l.id AS learn_id,
					lc.id AS learnclass_id,
					lu.user_id,
					l.team_id,
					lc.instructor_id,
					lc.by_groups,
					----------------questions          (dim_learn)    --- quantidade de perguntas
					lq.title AS question,
					la.title AS response,
					lq.learn_questions_bank_difficulty_id AS difficulty,
					lq.database, 
                    #CAST(lq.dateload AS TIMESTAMP) AS dateload,
					lq.op
			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id #AND la.weight IN(1)
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
      WHERE l.id = 47607
			and lu.user_id = 690868
			
			
			
			
			
####################################################################################			
######################## AGREGADAS ###############################################
####################################################################################
COLUNAS questions, answers, correct_answers e response_status

SELECT
  l.id AS learn_id,
  COUNT(lq.title) AS questions,
  COUNT(la.title) AS answers,
  COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) AS correct_answers,
  CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) AS assertiveness,
  SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) AS score_obtained,
	l.meta as goal, --- TIRAR DA QUERY FINAL
   CASE
					WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "Reprovado"
					WHEN DATE_DIFF(lc.finishin, CURRENT_TIMESTAMP(), HOUR) < 0 AND lu.response_date < TIMESTAMP("1970-01-01") THEN "Não entregue"
					WHEN lu.response_date > TIMESTAMP("1900-01-01") AND 
							CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id 
									AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
					ELSE "Checar regra"
			END AS response_status,
          lc.finishin, lu.response_date
			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id AND la.weight IN(1)
			LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
      WHERE l.id = 47607
			and lu.user_id = 690868
      group by l.id, lc.finishin, lu.response_date, l.meta
	  
	  
	  
	  
	<<<<<<<<<<<<<<<<<<                          abaixo        usar do OneNote	>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  
	  
	  
	  
	  
	  
	  
	  
SELECT
  l.id AS learn_id,
  COUNT(lq.title) AS questions,
  COUNT(la.title) AS answers,
  COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) AS correct_answers,
  CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) AS assertiveness_questions, --GOAL 1 soma as respostas corretas
	CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) coins, --GOAL 2   
	CASE
		WHEN l.learn_goal_type_id = 1 THEN CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) --GOAL 1
		WHEN l.learn_goal_type_id = 2 THEN CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) --"GOAL 2"
		ELSE 0
	END assertiveness,


	CASE
		WHEN l.learn_goal_type_id = 1 AND CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) >= l.meta THEN "Aprovado goal1" --GOAL 1
		WHEN l.learn_goal_type_id = 2 AND CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) >= sum(lq.points) THEN "aprovado goal 2"
		ELSE "nok"
	END result,

  SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) AS score_obtained, --colunas pontos_obtidos (score_obtained) é tant oque cosneguiu obter soma dos pontos obtidos (somar respostas corretas)
	SUM(lq.points) AS points, --coluna pontos é a soma de todos pontos que ele poderia obter na tabela questions
	l.meta as goal, --- TIRAR DA QUERY FINAL
   CASE
					WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "Reprovado" 
					WHEN DATE_DIFF(lc.finishin, CURRENT_TIMESTAMP(), HOUR) < 0 AND lu.response_date < TIMESTAMP("1970-01-01") THEN "Não entregue"
					WHEN lu.response_date > TIMESTAMP("1900-01-01") AND 
							CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id 
									AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
					ELSE "Checar regra"
			END AS response_status,

          lc.finishin, lu.response_date,

			lc.attempts - lu.attempt AS remaining_treis,
			#CASE
			#		WHEN lu.attempt = 1 AND "result" = "Não Entregue" THEN '+1'
			#ELSE "remaining_treis"
			END remaining_treis

			CASE 
				WHEN lu.attempt = 1 AND "result" = "Não Entregue" THEN 0
			ELSE lu.attempt
			END user_attempts,

	 		CASE 
				WHEN lu.attempt = 0 AND "result" = "Não Entregue" THEN 'Ainda não entregou'
				WHEN lu.attempt > 0 AND "result" = "Não Entregue" THEN 'Ele já reprovou pelo menos 1 vez'
				WHEN lu.attempt > 0 AND "remaining_tries" > 0 AND "result" = "Reprovado" THEN 'Ele já reprovou pelo menos 1 vez'
				WHEN lu.attempt > 0 AND "remaining_tries" = 0 AND "result" = "Reprovado" THEN 'Ele errou todas as tentativas'
				WHEN lu.attempt > 0 AND "result" = "Aprovado" THEN 'Ele acertou em alguma das tentativas'
				ELSE remaining_treis
			END result_attempts,

			FROM beegdata_dev_trusted.learn l
			INNER JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			INNER JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id AND la.weight IN(1)
			LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
      WHERE l.id = 47607
			and lu.user_id = 690868
      group by l.id, lc.finishin, lu.response_date, l.meta, l.learn_goal_type_id, lc.attempts, lu.attempt

--coluna pontos é a soma de todos pontos que ele poderia obter na tabela questions
--colunas pontos_obtidos (score_obtained) é tant oque cosneguiu obter soma dos pontos obtidos (somar respostas corretas) 

--- regra 1: response_date menor que finish e assertividade igual ou maior que a meta + Porcentagem de respostas certas ou porcentagem de pontos obtidos >>> 1 coluna learn_goal_type da leran se 1 porcentagem de perguntas corretas se for 2 porcentagem de pontos obtidos  sempre porcentagem em ambos os casos deve ser igual ou maior que a meta. se um contar as perguntas se 2 somar os pontos responstas certas



SELECT
  l.id AS learn_id,
  lc.id as learnclass_id,
  #lu.id AS user_id,
  lc.database,
  CASE 
    WHEN l.learn_goal_type_id = 1 THEN "qtd_perguntas"
    WHEN l.learn_goal_type_id = 1 THEN "qtd_beetcoins"
  END goal_type,
  COUNT(DISTINCT lq.title) AS questions,
  COUNT(la.title) AS answers,
  COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) AS correct_answers,
  CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) AS assertiveness_questions, --GOAL 1
	CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) coins, --GOAL 2
	CASE
		WHEN l.learn_goal_type_id = 1 THEN CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) --GOAL 1
		WHEN l.learn_goal_type_id = 2 THEN CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) --"GOAL 2"
		ELSE 0
	END assertiveness,
	CASE
		WHEN l.learn_goal_type_id = 1 AND CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) >= l.meta THEN "Aprovado goal1" --GOAL 1
		WHEN l.learn_goal_type_id = 2 AND CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) >= sum(lq.points) THEN "aprovado goal 2"
		ELSE "nok"
	END response_status, #NAO HAVERA MAIS
  SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) AS score_obtained, --colunas pontos_obtidos (score_obtained) é tant oque cosneguiu obter soma dos pontos obtidos (somar respostas corretas)
	SUM(lq.points) AS points, --coluna pontos é a soma de todos pontos que ele poderia obter na tabela questions
	l.meta as goal, --- TIRAR DA QUERY FINAL
   CASE
          WHEN lu.id IS NULL THEN "Não ingressado"
					WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "Reprovado" 
					WHEN DATE_DIFF(lc.finishin, CURRENT_TIMESTAMP(), HOUR) < 0 AND lu.response_date < TIMESTAMP("1970-01-01") THEN "Não entregue"
					WHEN l.learn_goal_type_id = 1  AND lu.response_date > TIMESTAMP("1900-01-01") AND 
							CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id 
									AND la.weight = 1 THEN la.id END) / COUNT(la.title) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
					WHEN l.learn_goal_type_id = 2 AND lu.response_date > TIMESTAMP("1900-01-01") AND 
							CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
			END AS result,

          lc.finishin, lu.response_date

			FROM beegdata_dev_trusted.learn l
			LEFT JOIN beegdata_dev_trusted.learnclass lc ON l.id = lc.learn_id
			LEFT JOIN beegdata_dev_trusted.learnclassuser lu ON lc.id = lu.learnclass_id #######
			LEFT JOIN beegdata_dev_trusted.learndrop ld ON l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.id = la.learnquestion_id #AND la.weight IN(1)
			LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.learn_goal_type_id =  lg.id
      #WHERE l.id = 55786 and lu.id IS NULL #REPRESENTA NÃO INGRESSADO DA COLUNA RESULT
      where l.id = 55786 and lc.id = 173107 #não tem null aqui mas tem na OBT \ não pode ingressar e não ingressar no mesmo curso,
      #WHERE l.id = 47607 and lu.user_id = 690868
      #WHERE l.id = 64615 and lu.user_id = 853264
      #WHERE l.id = 55786 and lu.user_id = 970018
      
      group by l.id, lc.id, lu.id, lc.finishin, lu.response_date, l.meta, l.learn_goal_type_id, lc.attempts, lu.attempt, lc.database
	  
	  
	  
#############################################
NOVO OFICIAL
############################################################
SELECT 
	*,
		CASE
				WHEN q.attempt = 1 AND q.result = "Não entregue" THEN '+1'
			  ELSE CAST(q.remaining_treis AS STRING)
			END remaining_treis,
		
		CASE 
				WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 0
				ELSE q.attempt
			END user_attempts,
	  
		CASE 
				WHEN q.attempt = 0 AND q.result = "Não entregue" THEN 'Ainda não entregou'
				WHEN q.attempt > 0 AND q.result = "Não entregue" THEN 'Ele já reprovou pelo menos 1 vez'
				WHEN q.attempt > 0 AND 
						 CASE WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 1 ELSE q.remaining_treis END > 0 
						 AND q.result = "Reprovado" THEN 'Ele já reprovou pelo menos 1 vez'
				WHEN q.attempt > 0 AND
					   CASE WHEN q.attempt = 1 AND q.result = "Não entregue" THEN 1 ELSE q.remaining_treis END = 0
						 AND q.result = "Reprovado" THEN 'Ele errou todas as tentativas'
				WHEN q.attempt > 0 AND q.result = "Aprovado" THEN 'Ele acertou em alguma das tentativas'
				ELSE CAST(remaining_treis AS STRING)
			END result_attempts,
FROM (
			SELECT
					l.id AS learn_id,
					lc.id AS learnclass_id,
					lu.user_id,
					CASE 
						WHEN l.learn_goal_type_id = 1 THEN "qtd_perguntas"
						WHEN l.learn_goal_type_id = 2 THEN "qtd_beetcoins"
					END goal_type,
					COUNT(lq.title) AS questions,
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
							WHEN DATE_DIFF(lc.finishin, CURRENT_TIMESTAMP(), HOUR) < 0 AND lu.response_date < TIMESTAMP("1970-01-01") THEN "Não entregue"
							WHEN l.learn_goal_type_id = 1  AND lu.response_date > TIMESTAMP("1900-01-01") AND 
										CAST(COUNT(CASE WHEN la.id = lr.learnanswer_id 
												AND la.weight = 1 THEN la.id END) / NULLIF(COUNT(la.title),0) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
							WHEN l.learn_goal_type_id = 2 AND lu.response_date > TIMESTAMP("1900-01-01") AND 
										CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / NULLIF(sum(lq.points),0) * 100 AS INTEGER) >= l.meta THEN "Aprovado"
							ELSE "Reprovado"
						END AS result,
					--lc.finishin, lu.response_date,
					lc.attempts,
					lu.attempt,
					lc.attempts - lu.attempt AS remaining_treis, total de tentativas total feitas e ainda pra fazer
					lc.database
			FROM beegdata_dev_trusted.learn l
			LEFT JOIN beegdata_dev_trusted.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
			LEFT JOIN beegdata_dev_trusted.learnclassuser lu ON lc.database = lu.database AND lc.id = lu.learnclass_id #######
			LEFT JOIN beegdata_dev_trusted.learndrop ld ON l.database = ld.database AND l.id = ld.learn_id
			LEFT JOIN beegdata_dev_trusted.learnquestion lq ON ld.database = lq.database AND ld.id = lq.learndrop_id
			LEFT JOIN beegdata_dev_trusted.learnanswer la ON lq.database = la.database AND lq.id = la.learnquestion_id AND la.weight IN(1)
			LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.database = lr.database AND la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
			INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.database = lg.database AND l.learn_goal_type_id =  lg.id
						#WHERE l.id = 47607 and lu.user_id = 690868 
						#WHERE l.id = 64615 AND lu.user_id = 1148989
						#WHERE l.id = 53721 AND lu.user_id = 878498
						#WHERE l.id = 64562  AND lu.user_id =  1003098 # ARPOVADO 1003098
						#WHERE l.id = 64562 and lu.user_id = 719164 # NAO ENTREGUE 719164
						#WHERE lu.user_id is null
						#WHERE l.id = 55790 AND lc.id = 190091
						#WHERE l.id = 64725 AND lc.id = 190263
			GROUP BY l.id, lc.id, lu.user_id, lc.finishin, lc.database, lu.id, lu.response_date, l.meta, l.learn_goal_type_id, lc.attempts, lu.attempt, lc.finishin, lu.response_date
) AS q 

#WHERE learn_id = 64804 AND user_id = 987688
#WHERE learn_id = 54135 AND user_id = 641676
#WHERE q.learn_id = 64566 AND user_id = 641517
#WHERE learn_id = 46765 and user_id = 1145509
#WHERE learn_id = 55003 AND user_id = 641633
WHERE learn_id = 63075 and user_id = 849919

lc.attempts - lu.attempt AS remaining_treis, total de tentativas total feitas e ainda pra fazer













VALIDADOR
#############################
SELECT fl.learn_id, fl.question, fl.response, fl.correct_answers, fl.difficulty, dl.drop_name, du.user_id, di.instructor_name FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` AS fl
LEFT JOIN `beegdata_dev_refined.dim_learnclass` AS dl ON fl.sk_learnclass = dl.sk_learnclass AND dl.scd_activate = 1
LEFT JOIN `beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user AND du.team_id = 480
LEFT JOIN `beegdata_dev_refined.dim_instructor` AS di ON di. sk_instructor = fl.sk_instructor
--LEFT JOIN `beegdata_dev_refined.dim_groupuser` AS dg ON dg.sk_groupuser = fl.sk_groupuser AND dg.team_id = 480  --AND rnk = 1 
WHERE fl.team_id >  0 AND dl.team_id >  0 AND dl.scd_activate = 1
--and learn_id = 32315
--and learnclass_id = 90368
--and user_id = 137971
--AND user_id = 836064
--and fl.learn_id = 52536
--and fl.user_id = 641567
--and fl.learn_id = 53596
--and fl.user_id = 864032
and fl.user_id = 847058
order by question, learnquestions_id