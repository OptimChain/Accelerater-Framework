# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Merge FCT_Store_Fuel_Tank
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

dbutils.widgets.text(name="containerName", defaultValue="cgs", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run ../Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions

# COMMAND ----------

spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="cgs", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="FCT_Store_Fuel_Tank", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")
timestampColumns = dbutils.widgets.get("timestampColumns")

enrichedPath = fullPathPrefix + "/Query/Enriched/" + schemaName + "/" + tableName
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

currentStateDestinationPath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + tableName

# COMMAND ----------

stageTableName = "Purchline"

databaseStageTableName = schemaName + "." + stageTableName

currentStateStagePath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + stageTableName


# COMMAND ----------

notebookName = "Query Zone Processing - Enrich"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Source Table Name: {0}".format(stageTableName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Destination Path: {0}".format(currentStateDestinationPath))
print("Current State Stage Path: {0}".format(currentStateStagePath))
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

try:
  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  dst_df = spark.read \
    .format("delta") \
    .load(currentStateDestinationPath) \
    .dropDuplicates()
except Exception as e:
  sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

display(dst_df)

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format(databaseTableName, currentStateDestinationPath)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseStageTableName)
spark.sql(sql)


sql = """
DROP TABLE IF EXISTS {0}
""".format("DIM_Alternate_UPC")
spark.sql(sql)

sql = """
DROP TABLE IF EXISTS {0}
""".format("DIM_Fuel_Tank")
spark.sql(sql)


# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format(databaseStageTableName, currentStateStagePath)
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("DIM_Alternate_UPC", fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + "DIM_Alternate_UPC")
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("DIM_Fuel_Tank", fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + "DIM_Fuel_Tank")
spark.sql(sql)

# COMMAND ----------

sql = """
SELECT *
FROM {0}
""".format(databaseStageTableName)
display(spark.sql(sql))

# COMMAND ----------

stgViewName =  schemaName + "." + "FCTStoreFuelTank" + "_view"

sql = """
DROP VIEW IF EXISTS {0}
""".format(stgViewName)
spark.sql(sql)


# COMMAND ----------

sql="""
CREATE OR REPLACE VIEW {0} AS
SELECT c.Store_SK,
       CAST(date_format(Date,"yyyyMMdd") as LONG) as Date_SK,
       b.Product_SK,
       c.Fuel_Tank_SK,
       'Query Zone Processing - Append FCT_Store_Fuel_Tank' as Created_By,	
       '' as Modified_By,	
       CAST(Current_Date() as String) as Last_Created,	
       CAST('1900-01-01 00:00:00.0000000' as String) as Last_Modified	
      FROM {1} as a
      RIGHT OUTER JOIN cgs.DIM_Alternate_UPC as b ON b.Product_UPC = a.GasFuelType
      INNER JOIN cgs.DIM_Fuel_Tank as c ON c.Store_SK = a.Store AND c.Tank_Number = a.Tank
      ORDER BY Store,
             Tank,
             Date,
             GasFuelType
""".format(stgViewName, databaseStageTableName)
display(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT *
  FROM {0}
""".format(stgViewName)
stg_df=(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT Date_SK, count(*)
  FROM {0}
  Group by Date_SK
""".format(stgViewName)
display(spark.sql(sql))

# COMMAND ----------

display(stg_df)

# COMMAND ----------

sql="""
CREATE OR REPLACE VIEW {0} AS
SELECT DISTINCT Date_SK FROM {1}
""".format("cgs.Incremental_FCT_Store_Fuel_Tank_DateLoaded",stgViewName)
display(spark.sql(sql))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cgs.Incremental_FCT_Store_Fuel_Tank_DateLoaded

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO cgs.FCT_Store_Fuel_Tank
# MAGIC USING cgs.Incremental_FCT_Store_Fuel_Tank_DateLoaded
# MAGIC ON cgs.FCT_Store_Fuel_Tank.Date_SK = cgs.Incremental_FCT_Store_Fuel_Tank_DateLoaded.Date_SK
# MAGIC WHEN MATCHED THEN
# MAGIC   DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append Data in Query Zone (Current State)

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
    (stg_df \
      .write \
      .mode("append") \
      .format("delta") \
      .save(currentStateDestinationPath)
    )
  else:
    (stg_df \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStateDestinationPath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(tableName, currentStateDestinationPath)
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
stageTableName = "Incremental_FCT_Store_Fuel_Tank_DateLoaded"

run_with_retry(sanctionedZoneNotebookPath, 1200, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "schemaName": stageSchemaName, "tableName": stageTableName, "numPartitions": numPartitions})

# COMMAND ----------

execsp = "DELETE cgs.FCT_Store_Fuel_Tank WHERE Date_SK in (SELECT Date_SK from stage.Incremental_FCT_Store_Fuel_Tank_DateLoaded)"
execute_sqldw_stored_procedure_no_results(execsp)

# COMMAND ----------

blob_storage_account_name = adlsGen2StorageAccountName
blob_storage_container_name = "temp"

tempDir = "abfss://{}@{}.dfs.core.windows.net/".format(blob_storage_container_name, blob_storage_account_name) + dbutils.widgets.get("tableName")

# COMMAND ----------

sqlDwUrlSmall, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------

try:
  stg_df \
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

dbutils.fs.rm(tempDir,True)

# COMMAND ----------

sql = """
DROP VIEW IF EXISTS {0}
""".format("cgs.Incremental_FCT_Store_Fuel_Tank_DateLoaded")
spark.sql(sql)

# COMMAND ----------

sql = """
DROP VIEW IF EXISTS {0}
""".format(stgViewName)
spark.sql(sql)

# COMMAND ----------

execsp = "IF OBJECT_ID('stage.Incremental_FCT_Store_Fuel_Tank_DateLoaded') IS NOT NULL DROP TABLE stage.Incremental_FCT_Store_Fuel_Tank_DateLoaded"
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