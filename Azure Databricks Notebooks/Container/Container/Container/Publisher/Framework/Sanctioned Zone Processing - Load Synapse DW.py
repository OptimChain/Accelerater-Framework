# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Sanctioned Zone Processing - Load Synapse DW
# MAGIC ###### Author: Mike Sherrill 9/10/19
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Table must exist in the Spark Catalog. 
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

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="containerName", defaultValue="rsm", label="Container Name")
dbutils.widgets.text(name="schemaName", defaultValue="rsm", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="DIM_Promotion", label="Table Name")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
schemaName = dbutils.widgets.get("schemaName")
containerName = dbutils.widgets.get("containerName")
tableName = dbutils.widgets.get("tableName")
databaseTableName = schemaName + "." + tableName
fullyQualifiedTableName = schemaName + "." + dbutils.widgets.get("tableName")
numPartitions = int(dbutils.widgets.get("numPartitions"))

fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

enrichedPath = fullPathPrefix + "/Query/Enriched/" + schemaName + "/"  + tableName

# COMMAND ----------

notebookName = "Sanctioned Zone Processing - Load Azure SQL DW"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

blob_storage_account_name = adlsGen2StorageAccountName
blob_storage_container_name = "temp"

tempDir = "abfss://{}@{}.dfs.core.windows.net/".format(blob_storage_container_name, blob_storage_account_name) + dbutils.widgets.get("tableName")

# COMMAND ----------

adlsClientId = adlsClientId
adlsCredential = adlsCredential 

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", adlsClientId)
spark.conf.set("fs.azure.account.oauth2.client.secret", adlsCredential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+adlsTenantId+"/oauth2/token")

# COMMAND ----------

tempDir

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Source Dataframe from Existing Table

# COMMAND ----------

sqlDwUrlSmall, connectionProperties = build_sqldw_jdbc_url_and_connectionProperties(sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword)

# COMMAND ----------

sqlDwUrlSmall

# COMMAND ----------

databaseTableName

# COMMAND ----------

try:
  spark.sql("SELECT * FROM " + databaseTableName)
  df = spark.table(databaseTableName)
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

execsp = "IF OBJECT_ID('" + fullyQualifiedTableName + "') IS NOT NULL TRUNCATE TABLE " + fullyQualifiedTableName
execute_sqldw_stored_procedure_no_results(execsp)

# COMMAND ----------

print(tempDir)
try:
  df \
    .write \
    .format("com.databricks.spark.sqldw") \
    .mode("append") \
    .option("url", sqlDwUrlSmall) \
    .option("dbtable", fullyQualifiedTableName) \
    .option("useAzureMSI","True") \
    .option("maxStrLength",4000) \
    .option("tempdir", tempDir) \
    .save()
except Exception as e:
  sourceName = "Sanctioned Zone Processing - Load Azure SQL DW: Load Azure SQL Data Warehouse"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete temporary Blob Storage files used by SQL DW Connector

# COMMAND ----------

dbutils.fs.rm(tempDir,True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Purview Lineage

# COMMAND ----------

enrichedPath


# COMMAND ----------

sourcePath = enrichedPath.replace("abfss://" + containerName + "@","https://").replace("Query/Enriched", containerName + "/Query/Enriched") + "/{SparkPartitions}"
destinationPath= "mssql://rsmedpovdbshggd.database.windows.net/rsm_ed_pov_dw/" + schemaName.lower() + "/" + tableName


# COMMAND ----------

print("Source Path {0}".format(sourcePath))
print("Destination Path {0}".format(destinationPath))

# COMMAND ----------

sourceAsset = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")

# COMMAND ----------

sourceAsset

# COMMAND ----------

destinationAsset = client.get_entity(qualifiedName=destinationPath,typeName="azure_sql_dw_table")


# COMMAND ----------

destinationAsset

# COMMAND ----------

if sourceAsset != {} and destinationAsset != {}:
  print('Create Purview Lineage')
  create_purview_lineage_sanction(sourcePath, destinationPath, notebookName, databaseTableName, str(notebookExecutionLogKey))

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