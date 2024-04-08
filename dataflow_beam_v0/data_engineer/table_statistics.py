-- Returns metadata for tables in a single dataset cria a tabela table_statistics.
SELECT #
  (SELECT "team" as table_name) AS table_name,
  (SELECT MAX(dateload) as max_date 
    FROM  `beegdata_dev_trusted.team`) AS max_date,
  (SELECT COUNT (*) as qtd_rows 
    FROM  `beegdata_dev_trusted.team`) as qtd_rows,
  (SELECT COUNT(*) as qtd_columns
    FROM `beegdata_dev_trusted.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_schema = 'beegdata_dev_trusted'
      AND table_name = 'team') as qtd_columns,
  (SELECT cast((size_bytes / 1045) as INTEGER)
    FROM beegdata_dev_trusted.__TABLES__ WHERE table_id='team') as size_kbytes
    
    
    
    
    
-- Verfica duplicados na tabela LEARN
SELECT ROUND(SUM(qtd)/2) AS qtd, 'learn' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learn
    GROUP BY id, database
    HAVING count(*) > 1) AS r

UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'learnclass' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learnclass
    GROUP BY id, database
    HAVING count(*) > 1) AS r

UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'learnclassuser' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learnclassuser
    GROUP BY id, database
    HAVING count(*) > 1) AS r

UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'learndrop' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learndrop
    GROUP BY id, database
    HAVING count(*) > 1) AS r

UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'learnquestion' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learnquestion
    GROUP BY id, database
    HAVING count(*) > 1) AS r

UNION ALL 

SELECT ROUND(SUM(qtd)/2) AS qtd, 'learnanswer' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.learnanswer
    GROUP BY id, database
    HAVING count(*) > 1) AS r
    
    
    
UNION ALL

################################################################################## meu time
SELECT ROUND(SUM(qtd)/2) AS qtd, 'user' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM beegdata_dev_trusted.user
    GROUP BY id, database
    HAVING count(*) > 1) AS r
    
UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'group' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM beegdata_dev_trusted.group
    GROUP BY id, database
    HAVING count(*) > 1) AS r
   
UNION ALL

SELECT ROUND(SUM(qtd)/2) AS qtd, 'groupuser' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        group_id,
        user_id,
        database
    FROM 	beegdata_dev_trusted.groupuser
    GROUP BY group_id, user_id, database
    HAVING count(*) > 1) AS r
    
    
SELECT ROUND(SUM(qtd)/2) AS qtd, 'mood_history' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        user_id,
        database
    FROM 	beegdata_dev_trusted.mood_history
    GROUP BY id, user_id, database
    HAVING count(*) > 1) AS r
    
  
 SELECT ROUND(SUM(qtd)/2) AS qtd, 'mood' AS tbl FROM (
    SELECT
        count(*) AS qtd,
        id,
        database
    FROM 	beegdata_dev_trusted.mood
    GROUP BY id, database
    HAVING count(*) > 1) AS r
    
    
 
 ####################################### refined
 --meu time
 SELECT COUNT(*), database, user_id FROM `focus-mechanic-321819.beegdata_dev_refined.dim_user`
WHERE team_id > 1
GROUP BY database, user_id
having count(*) > 1
   
---cursos
SELECT count(*), learn_id, learnclass_id, user_id FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus`
WHERE team_id >0
group by learn_id, learnclass_id, user_id
having count(*) > 1
    
SELECT count(*), learn_id, learnclass_id, user_id, learnquestions_id FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions`
WHERE team_id > 0
group by learn_id, learnclass_id, user_id, learnquestions_id
having count(*) > 1

SELECT count(*), database, learn_id, learnclass_id FROM `focus-mechanic-321819.beegdata_dev_refined.dim_learnclass`
GROUP BY database, learn_id, learnclass_id
HAVING COUNT(*) > 1
    
############################################## tabelas e suas colunas para criar o dataqualuty #################################
"chat":"datetime",
"chatinfo":"datetime",
"chatparticipant":"datetime",
"post","post_react":"datetime",
"postcomment":"datetime",
"postfeedback":"datetime",
"postrate":"datetime",
"postview":"datetime",
"group":"datetime",
"useraccess":"datetime",
"score":"datetime",
"quiz":"datetime",
"userquizanswer":"datetime",
"help":"datetime",
"helpread":"datetime",
"helpview":"datetime",
"learn":"created",
"learnclass":"publishin",
"learnclassgroup":"created",
"scorm_user_state_interaction":"datahora_gravacao",
"groupuser":"created_at",
"levelmodificationhistory":"created_at",
"mood_history":"created_at",
"nps_user_answer":"answer_date",
"uac":"create_date",
"user":"created",
"quizgroupuser":"created",
"helphistory":"date",
"is_help_read":"created_at"
"tag":"created",
"wiki_subcategories":"created_at"

"learn_goal_type":"",
"learnanswer":"",
"learnansweruser":"",
"learndetailall":"",
"learndrop":"",
"learndropcontent":"",
"learnquestion":"",
"learntype":"",
"scorm_user_state":"",
"categorypost":"",
"grouppost":"",
"hierarchy":"",
"react":"",
"customfield":"",
"customfieldlist":"",
"grouping":"",
"groupingoptions":"",
"higherbeneathtype":"",
"idiom":"",
"level":"",
"groupingvalues":"",
"mood":"",
"team":"",
"userfield":"",
"usertype":"",
"powerapp_beeai":"",
"helpquiz":"",
"quizanswer":"",
"quizgroup":"",
"quizpost":"",
"quizquestion":"",
"quizuser":"",
"grouphelp":"",
"help_has_subcategory":""
"help_status":"",
"helpcategory":"",
"helptag":"",
"helpview_origin":"",





"learnclassuser":"updated_at"
df = spark.read.format('delta').load('gs://beeai-prd-raw-zone/database/beeai/learnclassuser/')
df.select('id').distinct().where('database = "beedoo"  and updated_at < hoje').count()




######################################################
--verificar o id maximo de cada tabela e fazer um count, checar as diferenças | esta abordagem não é acertiva apenas orientativa
--escolher um user id aleatorio e comparar os resultados | melhor abordagem
-- usar um max id do destino e fazer igual ou menor que ele e um count | esta abordagem não é acertiva apenas orientativa

%sql
SELECT *
	--max(learnanswer_id),
	--count(*)
	--learnclass_id, user_id 
FROM silver_beeai_preprod.learnansweruser
WHERE database = "beedoo" and user_id = 26904
--group by learnclass_id, user_id 
order by learnanswer_id, learnclass_id

SELECT *
	#max(learnanswer_id),
	#count(*)
	#learnclass_id, user_id 
FROM learnansweruser
where user_id = 26904
#group by learnclass_id, user_id 
order by learnanswer_id, learnclass_id


##############################################


###################################
# compara a quantidade de linhas da tabela learn entre base e referencia, sendo pordução como referencia, considerando o agrupamento por dia.

%sql
SELECT 
	count(*),
	cast(created as date) as created
FROM silver_beeai_preprod.learn
WHERE database = "beedoo"
group by cast(created as date) 
order by created desc

SELECT 
	count(*),
	cast(created as date) as created
FROM learn
group by cast(created as date) 
order by created desc

###################################
# compara a quantidade de linhas da tabela learnclassuser entre base e referencia, sendo pordução como referencia, considerando o agrupamento por dia.
FOI FEITA ESTA VALIDACOES E DEU COM NOTA 10

%sql
select
count(*), cast(response_date as date) as response_date
from silver_beeai_preprod.learnclassuser
where database = "beedoo"
group by cast(response_date as date)
order by response_date desc

select
count(*), cast(response_date as date) as response_date
from beedoo.learnclassuser
group by cast(response_date as date)
order by response_date desc


###################################
# compara a quantidade de linhas da tabela learnclassuser entre base e referencia, sendo pordução como referencia, considerando o agrupamento por dia.
%sql
select
count(*),
cast(response_date as date) as response_date
from silver_beeai_preprod.learndetailall
where database = "beedoo"
group by cast(response_date as date)
order by response_date desc

select
count(*),
cast(response_date as date) as response_date
from learndetailall
group by cast(response_date as date)
order by response_date desc


%sql
select
	count(*),
	cast(datetime as date) as datetime
from silver_beeai_preprod.help
where database = "beedoo"
group by cast(datetime as date)
order by datetime desc

select
	count(*),
	cast(datetime as date) as datetime
from help
group by cast(datetime as date)
order by datetime desc


%sql
select
	count(*),
	cast(datetime as date) as datetime
from silver_beeai_preprod.helpview
where database = "beedoo"
group by cast(datetime as date)
order by datetime desc

select
	count(*),
	cast(datetime as date) as datetime
from helpview
group by cast(datetime as date)
order by datetime desc


%sql
select
	count(*),
	cast(date as date) as date
from silver_beeai_preprod.helphistory
where database = "beedoo"
group by cast(date as date)
order by date desc

select
	count(*),
	cast(date as date) as date
from helphistory
group by cast(date as date)
order by date desc

############################### MEU TIME

%sql
select
	count(*),
	cast(created as date) as date
from silver_beeai_preprod.user
where database = "beedoo"
group by cast(created as date)
order by date desc  

select
	count(*),
	cast(created as date) as date
from user
group by cast(created as date)
order by date desc

select
	count(*),
	cast(useredited_at as date) as useredited_at
from user
group by cast(useredited_at as date)
order by useredited_at desc

###################################### FEED

%sql
select
  count(*),
  cast(created_at as date) as created_at
from silver_beeai_preprod.post
where database = "beedoo"
group by cast(created_at as date)
order by created_at desc

select
  count(*),
  cast(created_at as date) as created_at
from post
group by cast(created_at as date)
order by created_at desc


%sql
select
  count(*),
  cast(datetime as date) as datetime
from silver_beeai_preprod.postview
where database = "beedoo"
group by cast(datetime as date)
order by datetime desc

select
  count(*),
  cast(datetime as date) as datetime
from postview
group by cast(datetime as date)
order by datetime desc

%sql
select
  count(*),
  cast(datetime as date) as datetime
from silver_beeai_preprod.postrate
where database = "beedoo"
group by cast(datetime as date)
order by datetime desc


############################################### VALIDACOES
SELECT fl.learn_id, fl.learnclass_id, fl.question, fl.response, fl.correct_answers, fl.difficulty, dl.drop_name, du.user_id, du.name, di.instructor_name FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` AS fl
LEFT JOIN `beegdata_dev_refined.dim_learnclass` AS dl ON fl.sk_learnclass = dl.sk_learnclass AND dl.scd_activate = 1
LEFT JOIN `beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user AND du.team_id = 480
LEFT JOIN `beegdata_dev_refined.dim_instructor` AS di ON di. sk_instructor = fl.sk_instructor
--LEFT JOIN `beegdata_dev_refined.dim_groupuser` AS dg ON dg.sk_groupuser = fl.sk_groupuser AND dg.team_id = 480  --AND rnk = 1 
WHERE fl.team_id >  0 AND dl.team_id >  0 AND dl.scd_activate = 1
AND fl.database = 'ambev'
AND fl.learn_id = 58206
and fl.user_id = 847058 --perguntas e respostas OK \ diffculty -1 = Não definido OK \ correct_answers = status resposta OK \ 
order by question, learnquestions_id

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` 
WHERE 1=1
AND database = 'ambev'
AND learn_id = 58206
and user_id = 847058
ORDER BY perguntas, respostas



SELECT fl.learn_id, fl.learnclass_id, fl.question, fl.response, fl.correct_answers, fl.difficulty, dl.drop_name, du.user_id, du.name, di.instructor_name FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` AS fl
LEFT JOIN `beegdata_dev_refined.dim_learnclass` AS dl ON fl.sk_learnclass = dl.sk_learnclass AND dl.scd_activate = 1
LEFT JOIN `beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user AND du.team_id = 480
LEFT JOIN `beegdata_dev_refined.dim_instructor` AS di ON di. sk_instructor = fl.sk_instructor
--LEFT JOIN `beegdata_dev_refined.dim_groupuser` AS dg ON dg.sk_groupuser = fl.sk_groupuser AND dg.team_id = 480  --AND rnk = 1 
WHERE fl.team_id >  0 AND dl.team_id >  0 AND dl.scd_activate = 1
AND fl.database = 'ambev'
AND fl.learn_id = 48647
and fl.user_id = 641618 
order by question, learnquestions_id

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` 
WHERE 1=1
AND database = 'ambev'
AND learn_id = 48647
and user_id = 641618
ORDER BY perguntas, respostas



SELECT fl.learn_id, fl.learnclass_id, fl.question, fl.response, fl.correct_answers, fl.difficulty, fl.by_groups, dl.drop_name, du.user_id, du.name, di.instructor_name FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` AS fl
LEFT JOIN `beegdata_dev_refined.dim_learnclass` AS dl ON fl.sk_learnclass = dl.sk_learnclass AND dl.scd_activate = 1
LEFT JOIN `beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user AND du.team_id = 480
LEFT JOIN `beegdata_dev_refined.dim_instructor` AS di ON di. sk_instructor = fl.sk_instructor
--LEFT JOIN `beegdata_dev_refined.dim_groupuser` AS dg ON dg.sk_groupuser = fl.sk_groupuser AND dg.team_id = 480  --AND rnk = 1 
WHERE fl.team_id >  0 AND dl.team_id >  0 AND dl.scd_activate = 1
AND fl.database = 'ambev'
AND fl.learn_id = 52536
and fl.user_id = 641538 --BY GROUS ZERO \ aparece resposta <> na OBT não \ não respondeu nenhuma
order by question, learnquestions_id

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` 
WHERE 1=1 and by_groups = '0'
AND database = 'ambev'
AND learn_id = 52536
and user_id = 641538  
ORDER BY perguntas, respostas



SELECT fl.learn_id, fl.learnclass_id, fl.question, fl.response, fl.correct_answers, fl.difficulty, fl.by_groups, dl.drop_name, du.user_id, du.name, di.instructor_name FROM `focus-mechanic-321819.beegdata_dev_refined.fact_learnquestions` AS fl
LEFT JOIN `beegdata_dev_refined.dim_learnclass` AS dl ON fl.sk_learnclass = dl.sk_learnclass AND dl.scd_activate = 1
LEFT JOIN `beegdata_dev_refined.dim_user` AS du ON du.sk_user = fl.sk_user AND du.team_id = 480
LEFT JOIN `beegdata_dev_refined.dim_instructor` AS di ON di. sk_instructor = fl.sk_instructor
--LEFT JOIN `beegdata_dev_refined.dim_groupuser` AS dg ON dg.sk_groupuser = fl.sk_groupuser AND dg.team_id = 480  --AND rnk = 1 
WHERE fl.team_id >  0 AND dl.team_id >  0 AND dl.scd_activate = 1
AND fl.database = 'ambev'
AND fl.learn_id = 64609
and du.user_id = 641615 -- bygroup igual a zero \ a dfificuldade precisa ser tratada de integer para string
order by question, learnquestions_id

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` 
WHERE 1=1 and by_groups = '0'
AND database = 'ambev'
AND dificuldade <> 'Não Definido'
AND learn_id = 64609
and user_id = 641615 
ORDER BY perguntas, respostas












#*#  #*#  #*#  #*#  #*#  #*#
SELECT * FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 58206
and fs.user_id = 847058 -- bygroup igual a zero \ a dfificuldade precisa ser tratada de integer para string

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 58206
and fs.user_id = 847058

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 58206
and fs.user_id = 847058

                                                                                ######### ????????????????????????w
SELECT * FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 52536
and fs.user_id = 641538 ## caso interressante para analisar a questão de acertos 

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 52536
and fs.user_id = 641538


SELECT * FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 64609
and fs.user_id = 641615 -- bygroup igual a zero \ a dfificuldade precisa ser tratada de integer para string

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma_perguntas` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 64609
and fs.user_id = 641615

SELECT * FROM `focus-mechanic-321819.beegdata_prod.cursos_obt_por_turma` fs
WHERE  fs.database = 'ambev' AND fs.team_id = 480
AND fs.learn_id = 64609
and fs.user_id = 641615

SELECT * FROM `focus-mechanic-321819.beegdata_dev_refined.facta_learnstatus`
WHERE database = 'ambev' AND team_id = 480
AND learn_id = 49690
AND user_id = 839232 ---------------------------onde tem zero coniscide com a por turma