# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Sanctioned Zone Processing - Load Synapse DW BCP
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
dbutils.widgets.text(name="containerName", defaultValue="cgs", label="Container Name")
dbutils.widgets.text(name="schemaName", defaultValue="cgs", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="T_DIM_Product", label="Table Name")
dbutils.widgets.text(name="numPartitions", defaultValue="16", label="Number of Partitions")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
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
print("Destination Server Name: {0}".format(sqldwservername))
print("Destination Database Name: {0}".format(sqldwdatabasename))
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

incrementalView = sourceTableName + '_Incremental_View'
incrementalView

# COMMAND ----------

destinationTableName = 'cgs.FCT_Ticket_Tender_Databricks_BCP'

# COMMAND ----------

sql = """
CREATE OR REPLACE VIEW {0} as
SELECT * FROM {1} 
WHERE Date_SK = '20200218'
AND Store_SK <> 'Store_SK'
""".format(incrementalView,sourceTableName)
spark.sql(sql)

# COMMAND ----------

sql = """
select count(*) from {0}
""".format(incrementalView)
display(spark.sql(sql))

# COMMAND ----------

#execsp = "IF OBJECT_ID('" + destinationTableName + "') IS NOT NULL TRUNCATE TABLE " + destinationTableName
#execute_sqldw_stored_procedure_no_results(execsp)


# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val sourcetablename:String = dbutils.widgets.get("containerName") + "." + dbutils.widgets.get("tableName") + "_Incremental_View"
# MAGIC val destinationtablename:String = "cgs.FCT_Ticket_Tender_Databricks_BCP"
# MAGIC //val destinationtablename:String = dbutils.widgets.get("schemaName") + "." + dbutils.widgets.get("tableName")
# MAGIC 
# MAGIC val collection = spark.table(sourcetablename)
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> sqldwservername,
# MAGIC   "databaseName" -> sqldwdatabasename,
# MAGIC   "dbTable"      -> destinationtablename,
# MAGIC   "user"         -> sqldwusername,
# MAGIC   "password"     -> sqldwpassword,
# MAGIC   "bulkCopyBatchSize" -> "10000",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "0"
# MAGIC ))
# MAGIC 
# MAGIC collection.bulkCopyToSqlDB(config)

# COMMAND ----------

sql = """
OPTIMIZE {0}
ZORDER by (date_SK)
""".format(sourceTableName)
display(spark.sql(sql))

# COMMAND ----------

sql = """
SELECT date_SK, count(*) FROM {0} 
group by date_SK
order by date_SK desc
""".format(sourceTableName)
display(spark.sql(sql))

# COMMAND ----------

#jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, analyticsdbname, username, pwd)
jdbcUrl, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------

print("jdbc url: {0}".format(jdbcUrl))
print("connection properties: {0}".format(connectionProperties))

# COMMAND ----------

#try:
#  df \
#    .write \
#    .option("maxStrLength",256) \
#    .jdbc(jdbcUrl, destinationTableName, 'overwrite', connectionProperties)
#except Exception as e:
#  sourceName = "Sanctioned Zone Processing - Load Synapse DW Destination: BCP"
#  errorCode = "400"
#  errorDescription = e.message
#  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
#  raise(e)

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