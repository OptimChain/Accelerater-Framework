# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite Delta Lake from Gzip
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  Incremental files must be appended to the store e.g. /raw/tablename/yyyy/mm/dd. 
# MAGIC 2. The source system must be able to produce change feeds of new or updated records. 
# MAGIC 3. The source system tables must have a primary key and this metadata must be updated in the Framework database databricks.TableMetaData
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="cgs", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

dbutils.widgets.text(name="rawDataPath", defaultValue="/Raw/Sales_DR/", label="Raw Data Path")
dbutils.widgets.text(name="readOption", defaultValue="/*", label="Read Option")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="budget", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

rawDataPath = dbutils.widgets.get("rawDataPath")
fullRawDataPath = fullPathPrefix + rawDataPath
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
fullRawFilePath = fullRawDataPath
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + rawDataPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

notebookName = "Query Zone Processing - Append Delta Lake"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(fullRawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infer Schema

# COMMAND ----------

#schema = get_table_schema(containerName, rawDataPath, tableName)
#schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data From Raw Zone

# COMMAND ----------

df = spark.read \
    .option("header","true") \
    .option("quote","|") \
    .csv(fullRawDataPath) 

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn("dateLoaded", F.current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + containerName))

# COMMAND ----------

try:
  queryTableExists = (spark.table(databaseTableName) is not None)
  metadata = spark.sql("DESCRIBE DETAIL " + databaseTableName)
  format = metadata.collect()[0][0]
  if format != "delta":
    sourceName = "Query Zone Processing - Append Delta Lake: Validate Query Table"
    errorCode = "400"
    errorDescription = "Table {0}.{1} exists but is not in Delta Lake format.".format(schemaName, databaseTableName)
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise ValueError(errorDescription)
except:
  queryTableExists = False

# COMMAND ----------

try:
  if queryTableExists:
    (df \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStatePath)
    )
  else:
    (df \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStatePath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStatePath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Append Databricks Delta: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize & Vacuum

# COMMAND ----------

#sql = """OPTIMIZE {0}""".format(databaseTableName)
#spark.sql(sql)

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Look for Bad Records

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Append Databricks Delta: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, databaseTableName)
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise ValueError(errorDescription)
except:
  print("success")

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