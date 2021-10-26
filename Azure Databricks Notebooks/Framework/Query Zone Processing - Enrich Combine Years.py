# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Enrich Combine Years
# MAGIC ###### Author: Mike Sherrill 7/25/19
# MAGIC 
# MAGIC This notebook combines Spark Tables from separate years into a single Query Zone (enriched) delta lake table.
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Spark tables from separate years must exist (created from ingest notebook)
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

dbutils.widgets.text(name="rawDataPath", defaultValue="cgs/Raw/Dataset1/", label="Raw Data Path")
dbutils.widgets.text(name="combineYears", defaultValue="2012,2013,2014,2015,2016,2017", label="Combine Years")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="Hospital_Patient_Origination_By_County", label="Table Name")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

rawDataPath = dbutils.widgets.get("rawDataPath")
fullRawDataPath = fullPathPrefix + rawDataPath
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
combineYears = dbutils.widgets.get("combineYears")
fullyQualifiedTableName = schemaName + "." + tableName
#fullRawFilePath = fullRawDataPath
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath
databaseTableName = containerName + "." + tableName


# COMMAND ----------

notebookName = "Query Zone Processing - Enrich Combine Years"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Combine Years: {0}".format(combineYears))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(fullRawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Enriched State Path: {0}".format(enrichedPath))
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
# MAGIC #### Read Data from Spark tables, and combine into a data frame

# COMMAND ----------

i=0
for fileYear in combineYears.split(','):
  filePath = currentStatePath + "_" + combineYears.split(',')[i]
  databaseTable = databaseTableName + "_" + combineYears.split(',')[i]
  print(filePath)
  sql = """
  CREATE TABLE IF NOT EXISTS {0}
  USING delta
  LOCATION '{1}'
  """.format(databaseTable, filePath)
  spark.sql(sql)
  i+=1

# COMMAND ----------

sql = """
select '2012' as YEAR, * from {0}_2012
UNION
select '2013' as YEAR, * from {0}_2013
UNION
select '2014' as YEAR, * from {0}_2014
UNION
select '2015' as YEAR, * from {0}_2015
UNION
select '2016' as YEAR, * from {0}_2016
UNION
select '2017' as YEAR, * from {0}_2017
""".format(databaseTableName)
combined_df=spark.sql(sql)


# COMMAND ----------

import pandas as pd 
%who_ls DataFrame 

# COMMAND ----------

"""
# NOT USED
i = 0
for fileYear in combineYears.split(','):
  dfYear = "df_" + combineYears.split(',')[i]
  filePath = currentStatePath + "_" + combineYears.split(',')[i]
  print(filePath)
  print(dfYear)
  i += 1
  try:
  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
    dfYear = spark.read \
    .option("badRecordsPath", badRecordsPath) \
    .format("delta") \
    .load(filePath) \
    .dropDuplicates()
    display(dfYear)
  except Exception as e:
    sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
    errorCode = "200"
    errorDescription = e.message
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise(e)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------

try:
  combined_df \
    .repartition(numPartitions) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(enrichedPath)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

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