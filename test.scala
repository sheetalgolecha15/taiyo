// Databricks notebook source

import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType,TimestampType,DateType}
import org.apache.spark.sql.DataFrame


// COMMAND ----------

// MAGIC %md
// MAGIC the File location should have these files:<br>
// MAGIC
// MAGIC 1. Ratings<br>
// MAGIC 2.Moveies_metadata<br>

// COMMAND ----------

/*
Parameters
FIle_location: the folder location where the files are placed
write_file_type and read_file_type: type the file_type csv,json,delta,parquet are accepted
*/
dbutils.widgets.text("file_location", "","")
dbutils.widgets.text("read_file_type",",")
dbutils.widgets.text("write_file_type",",")



def file_location = dbutils.widgets.get("file_location")
def read_file_type = dbutils.widgets.get("read_file_type")
def write_file_type = dbutils.widgets.get("write_file_type")

// COMMAND ----------

/*
FIle_name: should be provided with complete extention
*/

def read_file(file_name:String): DataFrame ={
  def df=spark.read.format("csv").option("multiline",true).option("escapeQuotes", "true").option("header",true).load(file_location+file_name)
  df
}

// COMMAND ----------



// COMMAND ----------

//read the meta data for movies
def df_covid_cases=read_file("Coronavirus_cases_daily_update.csv")
display(df_covid_cases)

// COMMAND ----------

//cases per country
def window_continent=Window.partitionBy("continent")
def df_case_country=df_covid_cases.groupBy("continent","location").agg(sum("new_cases").as("total_cases"),sum("new_deaths").as("total_deaths"))
.withColumn("cases_per_continent",sum("total_cases").over(window_continent)) //window function for cases per continent
.withColumn("deaths_per_continent",sum("total_deaths").over(window_continent)) //window function for deaths per continent
.withColumn("percentage_cases_per_country",$"total_cases"/$"cases_per_continent") //percentage cases per continent
.withColumn("percentage_deaths_per_country",$"total_deaths"/$"deaths_per_continent")//percentage deaths per continent

display(df_case_country)

// COMMAND ----------

//count of deaths and cases per country after the vaccination started
def window_vaccine_country=Window.partitionBy("location","filter_vaccined")

def df_first_vaccination=df_covid_cases
.withColumn("filter_vaccined",when($"new_vaccinations".isNull or $"new_vaccinations"===0,lit("Not_Yet_Vaccinated")).otherwise(lit("Vaccine_started")))
.withColumn("First_vaccination_date",min($"date").over(window_vaccine_country))
.select("location","First_vaccination_date","filter_vaccined").distinct()
.withColumn("Not_Yet_Vaccinated",when($"filter_vaccined"==="Not_Yet_Vaccinated",$"First_vaccination_date"))
.withColumn("Vaccine_started",when($"filter_vaccined"==="Vaccine_started",$"First_vaccination_date"))
.groupBy("location").agg(min("Not_Yet_Vaccinated").as("Not_Yet_Vaccinated"),min("Vaccine_started").as("Vaccine_started"))

def df_agg_on_vaccine=df_covid_cases.groupBy("continent","location","date").agg(sum("new_cases").as("total_cases"),sum("new_deaths").as("total_deaths"))
.join(df_first_vaccination,Seq("location"))
.withColumn("case_before_vaccine",when($"date"<$"Vaccine_started",$"total_cases").otherwise(lit("0")))
.withColumn("death_before_vaccine",when($"date"<$"Vaccine_started",$"total_deaths").otherwise(lit("0")))
.withColumn("case_after_vaccine",when($"date">$"Vaccine_started",$"total_cases").otherwise(lit("0")))
.withColumn("death_after_vaccine",when($"date">$"Vaccine_started",$"total_deaths").otherwise(lit("0")))
.groupBy("location","continent")
.agg(sum("case_before_vaccine").as("case_before_vaccine"),sum("death_before_vaccine").as("death_before_vaccine"),
sum("case_after_vaccine").as("case_after_vaccine"),sum("death_after_vaccine").as("death_after_vaccine")
)


// COMMAND ----------

//saves in all three format
df_agg_on_vaccine.write.format("delta").partitionBy("continent").save("abfss://<container_name@<DL-name>.dfs.core.windows.net/orbis/archive/")
df_agg_on_vaccine.write.format("csv").save("abfss://<container_name@<DL-name>.dfs.core.windows.net/orbis/archive/")
df_agg_on_vaccine.write.format("json").save("abfss://<container_name@<DL-name>.dfs.core.windows.net/orbis/archive/")
