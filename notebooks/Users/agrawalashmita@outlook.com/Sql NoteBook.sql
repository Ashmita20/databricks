-- Databricks notebook source
select * from winequality_red_1

-- COMMAND ----------

create database wine


-- COMMAND ----------

create table wine.wine_red 
as  select `fixed acidity` 	as	fixed_acidity,
`volatile acidity`	as	volatile_acidity,
`citric acid` 		as	citric_acid,
`residual sugar` 	as	residual_sugar,
`chlorides` 		as	chlorides,
`free sulfur dioxide`   as	free_sulfur_dioxide,
`total sulfur dioxide`  as	total_sulfur_dioxide,
`density`		as	density,
`pH` 			as	pH,
`sulphates` 		as	sulphates,
`alcohol` 		as	alcohol,
`quality` 		as	quality
  from default.winequality_red_1

-- COMMAND ----------

select * from wine.wine_red

-- COMMAND ----------

select count(1), min(volatile_acidity),max(fixed_acidity) from wine.wine_red

-- COMMAND ----------

select count(1),quality from wine.wine_red 
group by quality