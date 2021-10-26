# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Merge Using Snapse Template
# MAGIC ###### Author: Mike Sherrill 03/03/2020
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

# MAGIC %run ./Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ./Framework/Neudesic_Framework_Functions

# COMMAND ----------

spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="cgs", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="DIM_Cashier", label="Table Name")
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

notebookName = "Query Zone Processing - Merge DIM_Cashier"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Destination Path: {0}".format(currentStateDestinationPath))
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
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("STG_Cashier_Info", fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + "STG_Cashier_Info")
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("STG_Cashier_Employee_Info", fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + "STG_Cashier_Employee_Info")
spark.sql(sql)


# COMMAND ----------

try:
  spark.sql("REFRESH TABLE STG_Cashier_Info")
  dfc = spark.table("cgs.STG_Cashier_Info")
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load Azure SQL DW: Build Source Dataframe from Existing Table"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

try:
  spark.sql("REFRESH TABLE STG_Cashier_Employee_Info")
  dfcs = spark.table("STG_Cashier_Employee_Info")
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load Azure SQL DW: Build Source Dataframe from Existing Table"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Azure SQL Data Warehouse

# COMMAND ----------

sanctionedZoneNotebookPath = "../Framework/Sanctioned Zone Processing - Load Synapse DW Overwrite"
stageSchemaName = "stage"
stageTableName = "STG_Cashier_Info"

run_with_retry(sanctionedZoneNotebookPath, 1200, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "schemaName": stageSchemaName, "tableName": stageTableName, "numPartitions": numPartitions})

# COMMAND ----------

sanctionedZoneNotebookPath = "../Framework/Sanctioned Zone Processing - Load Synapse DW Overwrite"
stageSchemaName = "stage"
stageTableName = "STG_Cashier_Employee_Info"

run_with_retry(sanctionedZoneNotebookPath, 1200, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "schemaName": stageSchemaName, "tableName": stageTableName, "numPartitions": numPartitions})

# COMMAND ----------

dateLoaded = datetime.datetime.today()
print(dateLoaded)

# COMMAND ----------

execsp = "cgs.SP_ETL_Load_DIM_Cashier"
execute_sqldw_stored_procedure_no_results(execsp)


# COMMAND ----------

# MAGIC %md
# MAGIC #### DROP temporary Objects

# COMMAND ----------

execsp = "IF OBJECT_ID('stage.STG_Cashier_Info') IS NOT NULL DROP TABLE stage.STG_Cashier_Info"
execute_sqldw_stored_procedure_no_results(execsp)

execsp = "IF OBJECT_ID('stage.STG_Cashier_Employee_Info') IS NOT NULL DROP TABLE stage.STG_Cashier_Employee_Info" 
execute_sqldw_stored_procedure_no_results(execsp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrieve Changed and Inserted records from the Data Warehouse

# COMMAND ----------

sqlDwUrlSmall, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------


query = """(
    SELECT 
    *
    ,getdate() as dateLoaded
    FROM {0}
    WHERE  ( Last_Created > '{1}' or Last_Modified > '{1}' )
  ) t""".format(databaseTableName,dateLoaded)
stg_df = spark.read.jdbc(url=sqlDwUrlSmall, table=query, properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Upsert Table

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/cgs.db/DIM_Cashier_data_delta_to_upsert", True)

# COMMAND ----------

try:  
  #create temp table to use with spark sql
  stg_df.createOrReplaceTempView("raw")

  #build spark sql query to create neu_ metadata columns
  pk_cols = primaryKeyColumns.split(",")
  pk_string = "CONCAT("
  for c in pk_cols:
    col = "CAST(COALESCE(`" + c.strip() + "`,'') AS STRING),"
    pk_string += col
  pk_string = pk_string[0:-1] + ")"
  ts = "CAST(COALESCE(`" + timestampColumns.split(",")[0] + "`,'1900-01-01 12:00:00.0000000 + 00:00') AS STRING)"
  sql = """
  SELECT 
   *
   ,{0} as neu_pk_col
 FROM raw
  """.format(pk_string, ts)

  raw_df_with_metadata = spark.sql(sql)
  deltaTableName = databaseTableName + "_data_delta_to_upsert"
  spark.sql("DROP TABLE IF EXISTS " + deltaTableName)
  raw_df_with_metadata.write.saveAsTable(deltaTableName)
except Exception as e:
  sourceName = "Query Zone Processing - Merge Delta Lake: Build Upsert Table"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

#drop spark table, as it most likely is pointed at Enriched folder and does not have the neu_pk_col merge column
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
# MAGIC #### Read Existing Data from Query Zone (CurrentState}

# COMMAND ----------

print(currentStateDestinationPath)
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

# MAGIC %md
# MAGIC #### Merge Data in Query Zone (Current State)

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
    metadata = spark.sql("DESCRIBE DETAIL " + databaseTableName)
    format = metadata.collect()[0][0]
    if format != "delta":
      errorstring = "Table {0}.{1} exists but is not in Databricks Delta format.".format(schemaName, databaseTableName)
      raise ValueError(errorstring)
  else:
    (raw_df_with_metadata \
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

  if queryTableExists:
    sql = build_merge_SQL_Statement(deltaTableName, databaseTableName, raw_df_with_metadata.columns)
    print(sql)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Merge Delta Lake: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize and Vacuum Table

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