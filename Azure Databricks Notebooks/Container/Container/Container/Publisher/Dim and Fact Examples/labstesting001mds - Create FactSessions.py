# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # labstesting001mds - Create FactSessions
# MAGIC ###### Author: Mike Sherrill 11/13/19
# MAGIC 
# MAGIC This notebook is used to create the FactSessions from spark tables created during query zone enrich processing.  It creates spark tables and writes to the enriched zone.  
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

notebookName = "labstesting001mds - Create FactSessions"
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
# MAGIC #### Create FactSessions

# COMMAND ----------

tableName = "FactSessions"

databaseTableName = containerName + "." + tableName

enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Stg View

# COMMAND ----------

sql = """CREATE OR REPLACE VIEW labstesting001mds.StgFactSessionsView AS
SELECT 
   AssociatedUser
  ,MachineName
  ,DeliveryGroup
  ,SessionStartTime
  ,SessionEndTime
  ,SessionDuration
  ,"concurrent_connections" as SourceTableName
  FROM {0}
UNION 
SELECT 
   AssociatedUser
  ,MachineName
  ,DeliveryGroup
  ,SessionStartTime
  ,SessionEndTime
  ,SessionDuration
  ,"concurrent_desktop_analytics" as SourceTableName
FROM {1}
UNION 
SELECT 
   AssociatedUser
  ,MachineName
  ,DeliveryGroup
  ,SessionStartTime
  ,SessionEndTime
  ,SessionDuration
 ,"data_analytics_concurrent_sessions" as SourceTableName
FROM {2}
""".format("labstesting001mds.concurrent_connections","labstesting001mds.concurrent_desktop_analytics","labstesting001mds.data_analytics_concurrent_sessions")
df=spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Fact

# COMMAND ----------

sql = """
SELECT 
   COALESCE(p.DesktopUserKey,-1) as DesktopUserKey
   ,AssociatedUser
   ,MachineName
   ,DeliveryGroup
   ,SessionStartTime
   ,SessionEndTime
   ,SessionDuration
   ,"labstesting001mds - Create FactSessions" as CreatedBy
   ,current_timestamp() as CreatedDate
   ,timestamp(NULL) as UpdateDate
from {0} f
LEFT JOIN labstesting001mds.dimDesktopUser p on f.AssociatedUser = p.DesktopUserName
""".format("labstesting001mds.StgFactSessionsView")
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
  sourceName = "Create DimDesktopUser - Write to Query Zone (Enriched)"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Spark Fact Table

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