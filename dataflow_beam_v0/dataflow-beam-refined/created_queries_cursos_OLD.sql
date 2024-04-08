DROP TABLE IF EXISTS beegdata_dev_refined.dim_category;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_category(			
					category_id INTEGER, 
					category STRING,
					user_id INTEGER, 
					team_id INTEGER, 
					database STRING,
					dateload TIMESTAMP,
					sk_category BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = TRUE);
	


DROP TABLE beegdata_dev_refined.dim_learnclass;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_learnclass(			
      		learn_id INTEGER,
				  learnclass_id INTEGER,
					--user_id INTEGER,
					drop_name STRING,
					learn_description STRING,
					learn_status INTEGER,
					team_id INTEGER,
					classname STRING,
					class_description STRING,
					created_date TIMESTAMP,
					created_class TIMESTAMP,
				    by_groups INTEGER,
					average_time_finish INTEGER,
					selective_process INTEGER,
					publishin_date TIMESTAMP,
					finishin_date TIMESTAMP,
					class_status INTEGER,
					class_type INTEGER,
					database STRING, 
					dateload TIMESTAMP,
					--op STRING,
					sk_learnclass BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = TRUE);
	

DROP TABLE IF EXISTS beegdata_dev_refined.dim_students;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_students(			
					learnclass_id INTEGER,
					user_id INTEGER,
					by_groups INTEGER,
					team_id INTEGER,
					group_id STRING,
					group_name STRING,
					database STRING, 
					sk_students BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, user_id
  OPTIONS (
    require_partition_filter = TRUE, description = 'Example name and addresses table');

DROP TABLE IF EXISTS beegdata_dev_refined.dim_students2;
CREATE OR REPLACE TABLE beegdata_dev_refined.dim_students2(			
					learnclass_id INTEGER,
					user_id INTEGER,
					by_groups INTEGER,
					team_id INTEGER,
				    groupuser ARRAY<STRUCT <
					    group_id INTEGER,
					    group_name STRING
			        >>,
					database STRING, 
					sk_students BIGNUMERIC,
					scd_started TIMESTAMP,
					scd_finished TIMESTAMP,
					scd_activate INTEGER
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
  CLUSTER BY database, user_id
  OPTIONS (
    require_partition_filter = TRUE, description = 'Example name and addresses table');
	
SELECT
  user_id, learnclass_id,
  a.group_id,
  a.group_name
FROM
   `focus-mechanic-321819.beegdata_dev_refined.dim_students2` CROSS JOIN UNNEST(groupuser) AS a
   where team_id > 0
   and user_id = 137971
   order by user_id, learnclass_id, a.group_id
	


	
	

	
	
	
DROP TABLE IF EXISTS beegdata_dev_refined.fact_learnquestions;
CREATE OR REPLACE TABLE beegdata_dev_refined.fact_learnquestions(
	learnquestions_id INTEGER,
	learn_id INTEGER,
	learnclass_id INTEGER,
	user_id INTEGER,
	team_id INTEGER,
	by_groups INTEGER,
	question STRING,
	response STRING,
	correct_answers STRING,
	difficulty STRING,
	response_date TIMESTAMP,
	database STRING, 
    dateload TIMESTAMP,
	op STRING,
	sk_learnclass BIGNUMERIC,
	sk_user BIGNUMERIC,
	sk_instructor BIGNUMERIC,
	sk_students BIGNUMERIC,
	sk_category BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
  OPTIONS (
    require_partition_filter = FALSE, description = 'Example name and addresses table');
    
	
	

DROP TABLE IF EXISTS  beegdata_dev_refined.facta_learnstatus;
CREATE OR REPLACE TABLE beegdata_dev_refined.facta_learnstatus(
	learn_id INTEGER,
	learnclass_id INTEGER,
	user_id INTEGER,
	team_id INTEGER,
	--by_groups INTEGER,
	goal_type STRING,
	questions INTEGER,
	answers INTEGER,
	correct_answers INTEGER,
	assertiveness INTEGER,
	score_obtained INTEGER,
	points INTEGER,
	goal INTEGER,
	result STRING,
	response_date TIMESTAMP,
	attempts INTEGER,
	attempt INTEGER,
	remaining_treis STRING,
	result_attempts STRING,
	database STRING, 
    dateload TIMESTAMP,
	sk_learnclass BIGNUMERIC,
	sk_user BIGNUMERIC,
	sk_instructor BIGNUMERIC,
	sk_students BIGNUMERIC,
	sk_category BIGNUMERIC
)
PARTITION BY
  RANGE_BUCKET(team_id, GENERATE_ARRAY(1, 5000, 1))
CLUSTER BY database, dateload
OPTIONS (
    require_partition_filter = FALSE, description = 'Example name and addresses table');
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	
#########################################################################################################################################################
SELECT
	
	####################################### ANALISAR AS REGRAS ETAPA 1 #####################################################
			
			
	
	####################################### ANALISAR AS REGRAS ETAPA 2 #####################################################
	#total_time  -----------------------------------------VINHA DA LEARNDETAILL ALL   \\\\\\\\\\\\\\\\\\\\\\\\\         TABELA learnclassuser is_running_time VERFICAR DEPOIS     VERIFICAR DEPOIS  
	lt.total_tries, ------------------------ ANALISAR A REGRA        quantidade de tentativas
	
	----- Deixar para um segundo momento
	#total_tries_scorm  -----------------------------------------VINHA DA LEARNDETAILL ALL      \\\\\\\\\\\\\\ VERIFICAR DEPOIS
	#total_time_scorm  -----------------------------------------VINHA DA LEARNDETAILL ALL  \\\\\\\\\\\\\\\\\\\\\\\\\            VERIFICAR DEPOIS
		####################################### FIM DE ANALISAR AS REGRAS #####################################################
	
	
	############################## PROCURAR EM OUTRAS DIMENSÕES #########################
	#group_name -------------------- VEM DA TABELA GROUPS
    #lern_group_name -------------------- VEM DA TABELA GROUPS
	lc.instructor_id,                                                                  (dim_instrutor)
	#instrutor_name,  ------------------ VEM DA TABLE USER  OU INSTRUCTOR                        (dim_instrutor)
	#instrutor_login ------------------ VEM DA TABLE USER OU INSTRUCTOR TABLE                 (dim_instrutor)
	#score --------------------VEM DA TABELA USER,                                                                 (dim_user)
	
	#restric_user -----------------------a definir  TABELA USER QUASE PRONTO
	############################## FIM DE PROCURAR EM OUTRAS DIMENSÕES #########################	
	
	
	############################ IGNORAR AS COLUNAS ABAIXO #################################
	#subdomain ------------- TABELA team_id JÁ FOI CRIADA 
	#response_status ------------- VEM DE QUAL TABELA, \\\\\\\\\\\\ VISTO ACIMA 
	#participant_status--------------CASE WHEN da coluna response_date  \\\\\\\\\\\\ VISTO ACIMA 
		# learndetailall_id  -------------------------------------------------unitlizada
	# user_id as participant   -----------------------------------------VINHA DA LEARNDETAILL ALL DESATIVDAA
	#database ---------------------------a definir JÁ FOI CRIADA 
	#dateload ---------------------------a definir JÁ FOI CRIADA 
		#fast_sclass_status -----------------------------------------VINHA DA LEARNDETAILL ALL  \\\\ NAO VAI MAIS PRECISAR DELA

				
#participant_not_send --------------------------- VEM DA COLUNA FAST_RESULT  \\\ REPROVADO PORQUQE NÃO ENVIOU				
# fast_result  -----------------------------------------VINHA DA LEARNDETAILL ALL   >>> basicamente um numero que representa aprovado reprovado da coluna response_status. Então é traduzirr o result. (definitivamente aprovado 1, definitivamente reprovado status 2)
response_status ####################################################### alterar aqui
############################ FIM DAS IGNORADAS #################################	

		
	################################ FEITOS ABAIXO ###################################################################################
	l.id AS learn_id,
	lc.id AS class_id,
	lc.id AS learnclass_id,
	lu.user_id,
	l.title AS drop_name,
	l.description
	lc.status, # --------------------- CONFIRMAR SE VEM DA TABELA LEARN  (sim valor imutavel)
	l.team_id,
	#user_name ------------------ VEM DA TABLE USER                 (dim_user)
	#lastname ------------------ VEM DA TABLE USER                     (dim_user)
	#login ------------------ VEM DA TABLE USER                           (dim_user)
	#user_status ------------------ VEM DA TABLE USER                (dim_user)
	#cpf, ------------------ VEM DA TABLE USER                            (dim_user)
	#agentid, ------------------ VEM DA TABLE USER                      (dim_user)
	#usertype_id  ------------------ VEM DA TABLE USER               (dim_user)
	#user_type ------------------ VEM DA TABLE USER                  (dim_user)
	#email ------------------ VEM DA TABLE USER                        (dim_user)
	#category, ------------------ VEM DA TABLE USER                   (dim_user)
	#title (drop) ------------------ VEM DA TABLE USER                  (dim_user)
	#leader --------------------VEM DA TABELA USER                     (dim_user)
	#leader_name --------------------VEM DA TABELA USER            (dim_user)
	########### classtype as class_mode   -----------------------------------------VINHA DA LEARNDETAILL ALL   \\\\   COLUNA type DA TABELA LEARNCLASSS FEITO
	###########  created_date ------------------------------------------------------VEM DE QUAL TABELA? LEARN
	###########  created_class  -----------------------------------------VINHA DA LEARNDETAILL ALL                    \\\      DA TABELA LEARNCLASSS
	###########  sclass_status   -----------------------------------------VINHA DA LEARNDETAILL ALL  \\\\\\\\\\\\\\\\\\\ IGNORAR
	###########  fast_result # sclass_status   -----------------------------------------VINHA DA LEARNDETAILL ALL REPETIDO   \\\\\\\\\\\\\\\\\\\\ IGNORAR		
	########### lc.by_groups,
	########### lq.title AS question,
	########### la.title AS response,
	########### lq.learn_questions_bank_difficulty_id AS difficulty,
	########### l.average_time_finish,
	########### status as lstatus  -----------------------------------------VINHA DA LEARNDETAILL ALL     \\\\\\ STATUS CURSO TABELA LEARN
	###########  response_date  -----------------------------------------VINHA DA LEARNDETAILL ALL  \\\\\\\\\\\\\\\\\\         TABELA LU 
	###########  lc.publishin as publishin_date,
	########### lc.finishin as finishin_date,
	###########  lc.status as class_status,
	########### lc.type as class_type,
	###########  lc.description as class_description
	########### l.selective_process,
	########### lc.name as classname,
	########### lc.description,
	########### metas AS goal tabela learn_goal_type         OK                 (fato)
	########### code as goal_type tabela learn_goal_type                          (fato)
	-- feitos 03/07
	# questions -----------------------------------------VINHA DA LEARNDETAILL ALL >>>> FAZER JOIN COM LEARNDROP E DROP leanrCOMQUESTINOS PARA SABER QUANTAS QUESTÕES          (dim learn)
	#answers  -----------------------------------------VINHA DA LEARNDETAILL ALL >>>>>  SOMAR ONDE WEIHGTH FOR IGUAL A 1             (dim learn)
	#correct_answers -----------------------------------------VINHA DA LEARNDETAILL ALL   >>>> FAZER SUM DA LA2 WHEGHT
	# assertiveness  -----------------------------------------VINHA DA LEARNDETAILL ALL >>> PORCENTAGEM ENTRE CORRECTANSSERS E ANSWERS
	# score_obtained  -----------------------------------------VINHA DA LEARNDETAILL ALL  >>> FAZER SUM DA LQ2.POINS vale moedas EATA NQ QUESTION COLUNA POINTS
	

	# verficar o caso abaixo
				result -----------------------------------------VINHA DA LEARNDETAILL ALL >>>>>> DATA FINAL DO CURSO E DATA DE RESPOSTA DO USUARIO 
				                                                --SE A DATA FINAL DO CURSO FOR MENOR DO QUE AGORA SIGNICA QUE FOI REPROVADO COMO NÃO ENTREGUE, 
																--SE A DATA DE RESPOSTA DO USUARIO FOI PREENCHIDA TEM QUE VER O ASSERTIVENES COM A META DO CURSO SE ASSERTIVENESS FOR MAIOR QUE A META ESTÁ APROVADO.
					  CASE
					WHEN DATE_DIFF(lc.finishin, lu.response_date, DAY) < 0 THEN "reprovado" 
					  ELSE "APROVADO"
					END AS result,                              passa a valer a regra de response_status >>> coluna a mais se ja fez todas as tentativas ou não não fez nenhuma \\ salvar historico de quantas vezes reprovou na dim_learnclass
					
				
				


	#coins --------------------------- VEM DA COLUNA FAST_RESULT \\ somar os point da learnquestion.  E usado para saber se o cara acertou tudo ou não. Para cada pergunta ganha uma moedinha. e definido um minimo que que precisa acertar. Ver o status aprovado reporvado reprovado se por porcentagem ou por moeda. 
				    Verificando regra: CAST(SUM(CASE WHEN la.id = lr.learnanswer_id AND la.weight = 1 THEN lq.points ELSE 0 END) / sum(lq.points) * 100 AS INTEGER) assertiveness_coins, --GOAL 2 
	
		
	################################ FIM DOS FEITOS ###################################################################################
	

		

	
#l.user_id, l.team_id, ld.learn_id, ld.name as name_drop, lq.id as question_id, lq.title, la.*
FROM beedoo.learn l
INNER JOIN beedoo.learnclass lc ON l.id = lc.learn_id
INNER JOIN beedoo.learnclassuser lu ON lc.id = lu.learnclass_id #######
INNER JOIN beedoo.learndrop ld ON l.id = ld.learn_id
INNER JOIN beedoo.learnquestion lq ON ld.id = lq.learndrop_id
INNER JOIN beedoo.learnanswer la ON lq.id = la.learnquestion_id AND la.weight IN(1)
INNER JOIN INNER JOIN beedoo.learnanswer la2 ON lq.id = la2.learnquestion_id   
LEFT JOIN beedoo.learnansweruser lr ON la2.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
inner join INNER JOIN beedoo.learnquestion lq2 ON lq2.id = la2.learnquestion_id AND la2.wheitg = 1 
#INNER JOIN beedoo.learntries lt ON lu.id = lt.learnclassuser_id
INNER JOIN learn_goal_type lg ON l.learn_goal_type_id =  lg.id
AND l.id = 45980
#and lc.id = 201771
and lu.user_id  = 235425
CHAVE PRIMARIA ID DO CURSO ID DA TURMA ID DO USUARIO






SELECT
  *
FROM beegdata_dev_trusted.learn l
INNER JOIN beegdata_dev_trusted.learnclass lc ON l.database = lc.database AND l.id = lc.learn_id
INNER JOIN beegdata_dev_trusted.learnclassuser lu ON lc.database = lu.learnclass_id AND lc.id = lu.learnclass_id
FALTA INNER JOIN beegdata_dev_trusted.learnquestion lq ON ld.database = lq.database AND ld.id = lq.learndrop_id
INNER JOIN beegdata_dev_trusted.learnanswer la ON lq.database = la.database AND lq.id = la.learnquestion_id #AND la.weight IN(1)
LEFT JOIN beegdata_dev_trusted.learnansweruser lr ON la.database = lr.database AND la.id = lr.learnanswer_id AND lu.user_id = lr.user_id AND lc.id = lr.learnclass_id
FALTA #INNER JOIN beegdata_dev_trusted.learntries lt ON lu.id = lt.learnclassuser_id
INNER JOIN beegdata_dev_trusted.learn_goal_type lg ON l.database = lq.database AND l.learn_goal_type_id =  lg.id
WHERE 1 = 1
AND l.id = 45980
#and lc.id = 201771
and lu.user_id  = 235425







CURSOS OBT

from {silver_db_name}.learn A
INNER JOIN {silver_db_name}.learn_goal_type B ON B.id = A.learn_goal_type_id AND B.database = A.database
INNER JOIN {silver_db_name}.team TM on TM.id = A.team_id AND TM.database = A.database


1-- cursos_obt_participantes_participantes
Parece ser uma fato agrupada

2- cursos_obt_porgrupo_grupo
parece ser uma fato agrupada



3- cursos_obt_porgrupo_participantes
parece ser uma fato agrupada


4 - cursos_obt_porgrupos_resumo
parece ser uma fato agrupada


5 - cursos_obt_porparticipante_scorm
Tem algo que precisa ser construido


6 - cursos_obt_porparticipantes_participantes
parece ser uma fato agrupada


7 - cursos_obt_porparticipantes_resumo
parece ser uma fato agrupada


8 - cursos_obt_top5
parece ser uma fato agrupada