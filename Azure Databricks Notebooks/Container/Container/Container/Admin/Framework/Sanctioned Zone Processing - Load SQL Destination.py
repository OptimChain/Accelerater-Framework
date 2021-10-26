# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Sanctioned Zone Processing - Load SQL Destination
# MAGIC ###### Author: Eddie Edgeworth 4/8/19
# MAGIC ###### Modified for CLA:  Mike Sherrill 9/23/19      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Table must exist in the Spark Catalog. 
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ../Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="containerName", defaultValue="client000000001", label="Container Name")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="table", label="Table Name")
dbutils.widgets.text(name="numPartitions", defaultValue="16", label="Number of Partitions")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
sourceTableName = containerName + "." + tableName
destinationTableName = schemaName + "." + tableName
numPartitions = int(dbutils.widgets.get("numPartitions"))

# COMMAND ----------

notebookName = "Sanctioned Zone Processing - Load SQL Destination"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Source Table Name: {0}".format(sourceTableName))
print("Destination Table Name: {0}".format(destinationTableName))
print("Destination Server Name: {0}".format(dbserver))
print("Destination Database Name: {0}".format(analyticsdbname))
print("Number of Partitions (controls parallelism): {0}".format(numPartitions))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Source Dataframe from Existing Table

# COMMAND ----------

try:
  spark.sql("REFRESH TABLE " + sourceTableName)
  df = spark.table(sourceTableName)
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load SQL Destination: Build Source Dataframe from Existing Table"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Azure SQL or Azure SQL Managed Instance

# COMMAND ----------

execsp="TRUNCATE TABLE " + destinationTableName
execute_analytics_stored_procedure_no_results(execsp)


# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val sourcetablename:String = dbutils.widgets.get("containerName") + "." + dbutils.widgets.get("tableName")
# MAGIC val destinationtablename:String = dbutils.widgets.get("schemaName") + "." + dbutils.widgets.get("tableName")
# MAGIC 
# MAGIC val collection = spark.table(sourcetablename)
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
# MAGIC collection.bulkCopyToSqlDB(config)

# COMMAND ----------

jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, analyticsdbname, username, pwd)

# COMMAND ----------

print("jdbc url: {0}".format(jdbcUrl))
print("connection properties: {0}".format(connectionProperties))

# COMMAND ----------

try:
  df \
    .repartition(numPartitions) \
    .write \
    .option("maxStrLength",256) \
    .jdbc(jdbcUrl, destinationTableName, 'overwrite', connectionProperties)
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load SQL Destination: Load Azure SQL or Azure SQL Managed Instance"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

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