# MAGIC  
# MAGIC 
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite Traffic
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
dbutils.widgets.text(name="tableName", defaultValue="Traffic", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName

currentStateDestinationPath = fullPathPrefix + "/Query/CurrentState/" + tableName
RawDataPath  = fullPathPrefix + "/Raw/RetailNext/Traffic.json" 

badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

notebookName = "Query Zone Processing - Overwrite Traffic"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
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
# MAGIC #### Read Raw Data from Raw Path

# COMMAND ----------

#from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType,BooleanType,IntegerType
#schema = StructType([
#                  StructField('data', StructType([
#                                                  StructField('value', IntegerType(), True),
#                                                  StructField('validity', StringType(), True),
#                                                  StructField('index', IntegerType(), True),
#                                                  StructField('group', StructType(), True)
#                                    ])
#    ,True),
#    StructField('ok',BooleanType(),True)
#])

# COMMAND ----------

#try:
#  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
#  raw_df = spark.read \
#    .format('json') \
#    .schema(schema) \
#    .load(RawDataPath) \
#    .dropDuplicates()
#except Exception as e:
#  sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
#  errorCode = "200"
#  errorDescription = e.message
#  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
#  raise(e)

# COMMAND ----------

try:
  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  raw_df = spark.read \
    .format('json') \
    .load(RawDataPath) \
    .dropDuplicates()
except Exception as e:
  sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import explode
df2 = raw_df.select(raw_df.ok,explode(raw_df.metrics))

display(df2)

# COMMAND ----------

#df3 = df2.select(df2.ok,df2.col.ok,df2.col.name,explode(df2.col.data))

df3 = df2.select(df2.ok,explode(df2.col.data))


# COMMAND ----------

display(df3)

# COMMAND ----------

df4 = df3.select(df3.ok,df3.col.index,df3.col.validity,df3.col.value,df3.col.group.finish,df3.col.group.start,df3.col.group.type)

df4 = df4.withColumnRenamed("col.index","index") \
        .withColumnRenamed("col.validity","validity") \
        .withColumnRenamed("col.value","value") \
        .withColumnRenamed("col.group.finish","finish") \
        .withColumnRenamed("col.group.start","start") \
        .withColumnRenamed("col.group.type","type")
display(df4)

# COMMAND ----------

df4.createOrReplaceTempView("Traffic")

# COMMAND ----------

sql="""
SELECT *
FROM Traffic
"""
display(spark.sql(sql))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read STG Data from Query Zone (CurrentState}

# COMMAND ----------

sql = """
CREATE DATABASE IF NOT EXISTS {0}
""".format(containerName)
spark.sql(sql)

# COMMAND ----------

sql = """	
SELECT 	
    *,
  'Query Zone Processing - Overwrite DIMVendor' as Created_By,	
  '' as Modified_By,	
  Current_Date() as Last_Created,	
  '1900-01-01 00:00:00.0000000' as Last_Modified	
FROM Traffic	
""".format()	
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