# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
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
# MAGIC 4. You cannot use Merge if Overwrite was already used for the destination file (currentStatePath) You must delete the currentState Table (or alter it) to switch to merge operations
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

spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="cgs", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

dbutils.widgets.text(name="rawDataPath", defaultValue="cgs/Raw/Dataset1/", label="Raw Data Path")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="EE_MeterData", label="Table Name")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")
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
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")
timestampColumns = dbutils.widgets.get("timestampColumns")
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")


# COMMAND ----------

notebookName = "Query Zone Processing - Merge Delta Lake"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

if primaryKeyColumns == '':
  errorstring = "This loading pattern requires the primary key colum(s) to be saved as Notebook Parameters."
  raise ValueError(errorstring)

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(rawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))
print("Primary Key Columns: {0}".format(primaryKeyColumns))
print("Timestamp Columns: {0}".format(timestampColumns))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

fullRawDataPath

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

deltaTableName = databaseTableName + "_data_delta_to_upsert"

display(spark.sql("DROP TABLE IF EXISTS " + deltaTableName))


# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/cgs.db/fct_ticket_sales_data_delta_to_upsert", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Upsert Table

# COMMAND ----------

try:  
  #create temp table to use with spark sql
  df.createOrReplaceTempView("raw")

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
  ,{0} AS neu_pk_col
  FROM raw
  """.format(pk_string, ts)

  raw_df_with_metadata = spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Merge Delta Lake: Build Upsert Table"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + containerName))

# COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS " + databaseTableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

try:
  queryTableExists = (spark.table(databaseTableName) is not None)
except:
  queryTableExists = False

# COMMAND ----------

queryTableExists

# COMMAND ----------

#sql = build_merge_SQL_Statement(deltaTableName, tableName, raw_df_with_metadata.columns)
#spark.sql(sql)

# COMMAND ----------

try:
    (raw_df_with_metadata \
      .write \
      .mode("append") \
      .format("delta") \
      .option("overwriteSchema", True) \
      .save(currentStatePath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStatePath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Merge Delta Lake: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

sql="""
SELECT COUNT(*) FROM {0}
""".format(databaseTableName)
display(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT count(*) FROM {0}
""".format(databaseTableName)
display(spark.sql(sql))

# COMMAND ----------

#sql = """OPTIMIZE {0}""".format(databaseTableName)
#spark.sql(sql)



# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Merge Delta Lake: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, tableName)
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