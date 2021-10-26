# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Client000000001 - Create FactHospitalMetrics
# MAGIC ###### Author: Mike Sherrill 9/23/19
# MAGIC 
# MAGIC This notebook is used to create facts from spark tables created during query zone enrich processing.  It creates spark tables and writes to the enriched zone.  
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Data must exist in the Spark tables.   
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Framework

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client000000001", label="Container Name")
dbutils.widgets.text(name="schemaName", defaultValue="Definitive", label="Schema Name")
containerName = dbutils.widgets.get("containerName")


# COMMAND ----------

# MAGIC %run ./Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ./Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))
schemaName = dbutils.widgets.get("schemaName")


# COMMAND ----------

notebookName = "Client000000001 - Create FactHospitalMetrics"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

sourceTableName = containerName + "." + "hospital_historical_financial_and_clinical_metrics"

stageTableName = schemaName + "." + "Stghospitalhistoricalfinancialandclinicalmetrics"

tableName = "FactHospitalMetrics"

databaseTableName = containerName + "." + tableName

destinationTableName = schemaName + "." + tableName

enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Stage Dataframe

# COMMAND ----------

# MAGIC %scala
# MAGIC val sourcetablename:String = dbutils.widgets.get("containerName") + "." + "hospital_historical_financial_and_clinical_metrics"
# MAGIC 
# MAGIC val sql = """
# MAGIC SELECT 
# MAGIC 	   CATEGORY
# MAGIC       ,double(CBSA_AVERAGE) as CBSA_AVERAGE
# MAGIC       ,double(GROWTH_1_YEAR) as GROWTH_1_YEAR
# MAGIC       ,double(GROWTH_3_YEAR) as GROWTH_3_YEAR
# MAGIC       ,int(HOSPITAL_ID) as HOSPITAL_ID
# MAGIC       ,HOSPITAL_NAME
# MAGIC       ,METRIC_LABEL
# MAGIC       ,double(NATIONAL_AVERAGE) as NATIONAL_AVERAGE
# MAGIC       ,double(VALUE_2013) as VALUE_2013
# MAGIC       ,double(VALUE_2014) as VALUE_2014
# MAGIC       ,double(VALUE_2015) as VALUE_2015
# MAGIC       ,double(VALUE_2016) as VALUE_2016
# MAGIC       ,double(VALUE_2017) as VALUE_2017
# MAGIC       ,double(VALUE_2018) as VALUE_2018
# MAGIC 	  ,double(NULL) as VALUE_2019
# MAGIC 	  ,double(NULL) as VALUE_2020
# MAGIC 	  ,double(NULL) as VALUE_2021
# MAGIC 	  ,double(NULL) as VALUE_2022
# MAGIC 	  ,double(NULL) as VALUE_2023
# MAGIC 	  ,double(NULL) as VALUE_2024
# MAGIC 	  ,double(NULL) as VALUE_2025
# MAGIC 	  ,double(NULL) as VALUE_2026
# MAGIC 	  ,double(NULL) as VALUE_2027
# MAGIC 	  ,double(NULL) as VALUE_2028
# MAGIC 	  ,double(NULL) as VALUE_2029
# MAGIC 	  ,double(NULL) as VALUE_2030
# MAGIC 	  ,double(NULL) as VALUE_2031
# MAGIC 	  ,double(NULL) as VALUE_2032
# MAGIC 	  ,double(NULL) as VALUE_2033
# MAGIC 	  ,double(NULL) as VALUE_2034
# MAGIC 	  ,double(NULL) as VALUE_2035
# MAGIC 	  ,double(NULL) as VALUE_2036
# MAGIC 	  ,double(NULL) as VALUE_2037
# MAGIC 	  ,double(NULL) as VALUE_2038
# MAGIC   FROM client000000001.hospital_historical_financial_and_clinical_metrics
# MAGIC """.format(sourcetablename)
# MAGIC val df=spark.sql(sql)

# COMMAND ----------

jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, analyticsdbname, username, pwd)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to SQL Server for pivot processing

# COMMAND ----------

execsp="TRUNCATE TABLE " + stageTableName
execute_analytics_stored_procedure_no_results(execsp)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val destinationtablename:String = dbutils.widgets.get("schemaName") + ".Stghospitalhistoricalfinancialandclinicalmetrics"
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> dbserver,
# MAGIC   "databaseName" -> analyticsdbname,
# MAGIC   "dbTable"      -> destinationtablename,
# MAGIC   "user"         -> username,
# MAGIC   "password"     -> pwd,
# MAGIC   "bulkCopyBatchSize" -> "5000",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "1800"
# MAGIC ))
# MAGIC 
# MAGIC df.bulkCopyToSqlDB(config)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Execute SQL Server Pivot Stored Procedure

# COMMAND ----------

execsp="Definitive.uspFactHospitalMetricsPivot"
execute_analytics_stored_procedure_no_results(execsp)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Query Pivot Results from SQL Server

# COMMAND ----------

query = """(
    SELECT 
    *
    FROM {0}
  ) t""".format(destinationTableName)
df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Spark Table From Pivot Results

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

df \
  .write \
  .mode("overwrite") \
  .saveAsTable("client000000001.HospitalMetricsPivotResults")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Fact Table in Spark

# COMMAND ----------

sql = """
select
     COALESCE(p.ProviderID, '-1') as ProviderKey
    ,f.*
    ,"Client000000001 - Create FactHospitalMetrics" as CreatedBy
    ,current_timestamp() as CreatedDate
    ,timestamp("") as UpdatedDate
from client000000001.HospitalMetricsPivotResults f
LEFT JOIN client000000001.dimprovider p on f.ProviderID = p.ProviderID
""".format(sourceTableName)
df=spark.sql(sql)

# COMMAND ----------

df \
  .write \
  .mode("overwrite") \
  .saveAsTable(databaseTableName)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Fact to Query Zone (Enriched)

# COMMAND ----------

try:
  df \
    .repartition(numPartitions) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(enrichedPath)
except Exception as e:
  sourceName = "Create FactHospitalMetrics - Write to Query Zone (Enriched)"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean-Up: Drop Spark Pivot Results Table

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS client000000001.HospitalMetricsPivotResults
"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName="")
dbutils.notebook.exit("Succeeded")