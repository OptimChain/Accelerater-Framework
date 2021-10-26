# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Append FCTPurchase
# MAGIC ###### Author: Chaitanya Badam 06/01/2020
# MAGIC 
# MAGIC Data Lake pattern for tables that are staged and merged into the final enriched version.  Takes a file from the raw data path and overwrites the table in the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Data must exist in the Data Lake /query zone (current State).   
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Framework

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="brtl", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run ../Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions

# COMMAND ----------

spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="32", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="brtl", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="PurchTable", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

destinationTableName = 'FCTPurchase'
tableName = 'PurchTable'

schemaName = dbutils.widgets.get("schemaName")
fullyQualifiedTableName = schemaName + "." + destinationTableName
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")
timestampColumns = dbutils.widgets.get("timestampColumns")

enrichedPath = fullPathPrefix + "/Query/Enriched/" + destinationTableName
databaseTableName = containerName + "." + destinationTableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName

# COMMAND ----------

notebookName = "Query Zone Processing - Append FCTPurchase"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
# print("Source Table Name: {0}".format(stageTableName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Path: {0}".format(currentStatePath))
# print("Current State Stage Path: {0}".format(currentStateStagePath))
print("Enriched State Path: {0}".format(enrichedPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Existing Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(tableName)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format(tableName, currentStatePath)
spark.sql(sql)

# COMMAND ----------

sql = """
SELECT *
FROM {0}
""".format(tableName)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

stgViewName =  "brtl.Incremental_Purchase"

sql="""
CREATE OR REPLACE VIEW {0} AS
SELECT *
,'Query Zone Processing - Append Purchase' AS Created_By
,'' AS Modified_By
,cast(current_date() AS string) AS Last_Created
--,'1900-01-01 00:00:00.0000000' AS Last_Modified
FROM {1}
""".format(stgViewName,tableName)
display(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT *
  FROM {0}
""".format(stgViewName)
stg_df=(spark.sql(sql))

# COMMAND ----------

try:
  #read enrichedPath into a dataframe if it exists, otherwise create it
  dst_df = spark.read \
    .format("delta") \
    .load(enrichedPath) \
    .dropDuplicates()
except Exception as e:
   (stg_df \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(enrichedPath)
    )

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE {0}
USING delta
LOCATION '{1}'
""".format(databaseTableName, enrichedPath)
spark.sql(sql)

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO brtl.FCTPurchase
# MAGIC USING brtl.Incremental_Purchase
# MAGIC ON brtl.FCTPurchase.PURCHID = brtl.Incremental_Purchase.PURCHID
# MAGIC AND brtl.FCTPurchase.DATAAREAID = brtl.Incremental_Purchase.DATAAREAID
# MAGIC AND brtl.FCTPurchase.PARTITION = brtl.Incremental_Purchase.PARTITION
# MAGIC WHEN MATCHED THEN
# MAGIC   DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append Data in Query Zone (Enriched)

# COMMAND ----------

try:
  queryTableExists = (spark.table(databaseTableName) is not None)
except:
  queryTableExists = False

# COMMAND ----------

queryTableExists

# COMMAND ----------

try:
  if queryTableExists:
    (stg_df.repartition(numPartitions) \
      .write \
      .mode("append") \
      .option("mergeSchema", True) \
      .format("delta") \
      .save(enrichedPath)
    )
  else:
    (stg_df.repartition(numPartitions) \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(enrichedPath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(destinationTableName, enrichedPath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Append Databricks Delta: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append Data in to Synapse DW from Current State

# COMMAND ----------

sanctionedZoneNotebookPath = "../Framework/Sanctioned Zone Processing - Load Synapse DW Overwrite"
stageSchemaName = "stage"
stageTableName = "Incremental_Purchase"

run_with_retry(sanctionedZoneNotebookPath, 0, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "schemaName": stageSchemaName, "tableName": stageTableName, "numPartitions": numPartitions})

# COMMAND ----------

execsp = "DELETE brtl.FCTPurchase WHERE [PURCHID] in (SELECT [PURCHID] from stage.Incremental_Purchase) \
                                     AND DATAAREAID in (SELECT DATAAREAID from stage.Incremental_Purchase) \
                                     AND PARTITION in (SELECT PARTITION from stage.Incremental_Purchase)"
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
  stg_df \
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

dbutils.fs.rm(tempDir,True)

# COMMAND ----------

sql = """
DROP VIEW IF EXISTS {0}
""".format("brtl.Incremental_Purchase")
spark.sql(sql)

# COMMAND ----------

sql = """
DROP VIEW IF EXISTS {0}
""".format(stgViewName)
spark.sql(sql)

# COMMAND ----------

execsp = "IF OBJECT_ID('stage.Incremental_Purchase') IS NOT NULL DROP TABLE stage.Incremental_Purchase"
execute_sqldw_stored_procedure_no_results(execsp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum Table

# COMMAND ----------

sql="""OPTIMIZE {0}""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

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