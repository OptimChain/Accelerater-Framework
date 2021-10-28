# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # labstesting001mds - Create FactDataAnalyticsDesktopUsage
# MAGIC 
# MAGIC This notebook is used to create FactDataAnalyticsDesktopUsage from spark tables created during query zone enrich processing.  It creates spark tables and writes to the enriched zone.  
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

dbutils.widgets.text(name="containerName", defaultValue="labstesting001mds", label="Container Name")
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


# COMMAND ----------

notebookName = "labstesting001mds - Create FactDataAnalyticsDesktopUsage"
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

# MAGIC %md
# MAGIC #### Create FactDataAnalyticsDesktopUsage 

# COMMAND ----------

sourceTableName = containerName + ".Data_Analytics_Desktop_Usage"

tableName = "FactDataAnalyticsDesktopUsage"

databaseTableName = containerName + "." + tableName

enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName

# COMMAND ----------

sql = """
SELECT
     COALESCE(p.DesktopUserKey, '-1') as DesktopUserkey
    ,f.UserName
    ,File as Year
    ,AverageUsageDurationhrmin
    ,TotalInstances
    ,TotalLaunches
    ,TotalUsageDurationhrmin
    ,"labstesting001mds - Create FactDataAnalyticsDesktopUsage" as CreatedBy
    ,current_timestamp() as CreatedDate
    ,timestamp(NULL) as UpdateDate
from {0} f
LEFT JOIN labstesting001mds.dimDesktopUser p on f.UserName = p.DesktopUserName
""".format(sourceTableName)
df=spark.sql(sql)

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
  sourceName = "Create FactDataAnalyticsDesktopUsage - Write to Query Zone (Enriched)"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

  sql = """
  CREATE TABLE IF NOT EXISTS {0}
  USING delta
  LOCATION '{1}'
  """.format(databaseTableName, enrichedPath)
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