# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite DIMVendor
# MAGIC ###### Author: Ranga Bondada 05/25/2020
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  
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

#dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="brtl", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

# filePath is to pick up the schema of an actual file in the case of subfolders

dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="brtl", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="DIMVendor", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName

currentStateDestinationPath = fullPathPrefix + "/Query/Enriched/" + tableName

badRecordsPath = "/BadRecords/" + schemaName + "/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

stageTableName = "PurchTable"

databaseStageTableName = containerName + "." + stageTableName

currentStateStagePath = fullPathPrefix + "/Query/Enriched/"  + stageTableName


# COMMAND ----------

notebookName = "Query Zone Processing - Overwrite DIMVendor"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Stage Table Name: {0}".format(databaseStageTableName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Stage Path: {0}".format(currentStateStagePath))
print("Current State Destination Path: {0}".format(currentStateDestinationPath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))
print("Database Table Name: {0}".format(databaseTableName))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseStageTableName)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE DATABASE IF NOT EXISTS {0}
""".format(containerName)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format(databaseStageTableName, currentStateStagePath)
spark.sql(sql)

# COMMAND ----------

sql = """
SELECT *
FROM {0}
""".format(databaseStageTableName)
display(spark.sql(sql))

# COMMAND ----------

sql = """	
SELECT 	DISTINCT
  PURCHNAME as VendorName ,
  VENDGROUP as VendorGroup,
  VENDORREF as VendorReference,
  'Query Zone Processing - Overwrite DIMVendor' as Created_By,	
  '' as Modified_By,	
  Current_Date() as Last_Created,	
  '1900-01-01 00:00:00.0000000' as Last_Modified	
FROM {0}	

""".format(databaseStageTableName)	
dest_df=spark.sql(sql)

# COMMAND ----------

display(dest_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------


display(spark.sql("DROP TABLE IF EXISTS " + databaseTableName))

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/cgs.db/DIMVendor", True)

# COMMAND ----------

try:
  queryTableExists = (spark.table(tableName) is not None)
  metadata = spark.sql("DESCRIBE DETAIL " + databaseTableName)
  format = metadata.collect()[0][0]
  if format != "delta":
    sourceName = "Query Zone Processing - Overwrite Delta Lake: Validate Query Table"
    errorCode = "400"
    errorDescription = "Table {0}.{1} exists but is not in Databricks Delta format.".format(schemaName, tableName)
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise ValueError(errorDescription)
except:
  queryTableExists = False

# COMMAND ----------

queryTableExists

# COMMAND ----------

try:
  if queryTableExists:
    (dest_df \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStateDestinationPath)
    )
  else:
    (dest_df \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStateDestinationPath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStateDestinationPath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

sql = """
SELECT * FROM {0}
""".format(databaseTableName)
spark.sql(sql)


# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, tableName)
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise ValueError(errorDescription)
except:
  print("success")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append Data in to Synapse DW from Current State

# COMMAND ----------

execsp = "DELETE brtl.DIMVendor"
try:
  execute_sqldw_stored_procedure_no_results(execsp)
except:
  sourceName = "Destination Table does not exist"

# COMMAND ----------

blob_storage_account_name = adlsGen2StorageAccountName
blob_storage_container_name = "temp"

tempDir = "abfss://{}@{}.dfs.core.windows.net/".format(blob_storage_container_name, blob_storage_account_name) + dbutils.widgets.get("tableName")

# COMMAND ----------

sqlDwUrlSmall, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------

try:
  dest_df \
    .repartition(numPartitions) \
    .write \
    .format("com.databricks.spark.sqldw") \
    .mode("append") \
    .option("url", sqlDwUrlSmall) \
    .option("dbtable", fullyQualifiedTableName) \
    .option("useAzureMSI","True") \
    .option("maxStrLength",2048) \
    .option("tempdir", tempDir) \
    .save()
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load Azure SQL DW: Load Synapse SQL Data Warehouse"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Temporary Table and Views

# COMMAND ----------

execsp = "IF OBJECT_ID('stage.DIMVendor') IS NOT NULL DROP TABLE stage.DIMVendor"
execute_sqldw_stored_procedure_no_results(execsp)

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