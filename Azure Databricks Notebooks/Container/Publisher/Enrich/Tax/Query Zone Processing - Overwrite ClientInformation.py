# MAGIC   
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite ClientInformation
# MAGIC ###### Author: Ranga Bondada
# MAGIC 
# MAGIC Data Lake pattern for master data or small tables that can be overwritten every time.  Takes a file from the raw data path and overwrites the table in the Query zone.      
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

dbutils.widgets.text(name="containerName", defaultValue="rsm", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run rsm/Publisher/Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="rsm", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="AccountsNotAssignedToTaxCodes", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
enrichedPath = fullPathPrefix + "/Query/Enriched/Tax/" + tableName
databaseTableName = schemaName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

notebookName = "Query Zone Processing - Overwrite ClientInformation"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Path: {0}".format(currentStatePath))
print("Enriched State Path: {0}".format(enrichedPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS caseware"))

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format("ClientService")
spark.sql(sql)


sql = """
DROP TABLE IF EXISTS {0}
""".format("EngagementType")
spark.sql(sql)
sql = """
DROP TABLE IF EXISTS {0}
""".format("Engagement")
spark.sql(sql)

sql = """
DROP TABLE IF EXISTS {0}
""".format("t_client_SIC")
spark.sql(sql)


sql = """
DROP TABLE IF EXISTS {0}
""".format("Industry")
spark.sql(sql)

sql = """
DROP TABLE IF EXISTS {0}
""".format("SourceClientExtended")
spark.sql(sql)


sql = """
DROP TABLE IF EXISTS {0}
""".format("SourceClient")
spark.sql(sql)


sql = """
DROP TABLE IF EXISTS {0}
""".format("LegalStatus")
spark.sql(sql)

sql = """
DROP TABLE IF EXISTS {0}
""".format("t_NAICS")
spark.sql(sql)



	


# COMMAND ----------

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("ClientService", fullPathPrefix + "/Query/CurrentState/ClientService")
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("EngagementType",fullPathPrefix + "/Query/CurrentState/EngagementType")
spark.sql(sql)
sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("t_client_SIC",fullPathPrefix + "/Query/CurrentState/t_client_SIC")
spark.sql(sql)
sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("Engagement",fullPathPrefix + "/Query/CurrentState/Engagement")
spark.sql(sql)
sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("Industry",fullPathPrefix + "/Query/CurrentState/Industry")
spark.sql(sql)
sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("SourceClientExtended",fullPathPrefix + "/Query/CurrentState/SourceClientExtended")
spark.sql(sql)
sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("SourceClient",fullPathPrefix + "/Query/CurrentState/SourceClient")
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("LegalStatus",fullPathPrefix + "/Query/CurrentState/LegalStatus")
spark.sql(sql)

sql = """
CREATE TABLE IF NOT EXISTS {0}
USING delta
LOCATION '{1}'
""".format("t_NAICS",fullPathPrefix + "/Query/CurrentState/t_NAICS")
spark.sql(sql)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from ClientService
# MAGIC ---> take the row with the latest “date_begin” and grab the column “naics_code” -> t_NAICS.title.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM SourceClient

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT e.ClientNumber
# MAGIC          , e.FiscalYearEnd	
# MAGIC          , e.EngagementName
# MAGIC          , e.DoNotDisclose	
# MAGIC          , et.EngagementTypeName	
# MAGIC          , tcs.naics_code
# MAGIC          , sce.Tax_form	
# MAGIC          , sce.Tax_Status	
# MAGIC          , sce.SEC	
# MAGIC          , '' Contact	--Empty
# MAGIC          , i.IndustryName
# MAGIC          , sc.SEC_id	
# MAGIC          , cs.Name	
# MAGIC          , ls.LegalStatusName
# MAGIC          , n.title
# MAGIC 
# MAGIC FROM Engagement e 
# MAGIC INNER JOIN EngagementType et ON et.EngagementTypeID = e.EngagementTypeID
# MAGIC INNER  JOIN t_client_SIC tcs ON tcs.client_number = e.ClientNumber  
# MAGIC INNER JOIN SourceClientExtended sce ON sce.ClientNumber = e.ClientNumber
# MAGIC INNER JOIN Industry i ON i.IndustryID = e.IndustryID
# MAGIC INNER JOIN SourceClient sc ON sc.SEC_id = sce.SEC_id and sc.ClientNumber = e.ClientNumber
# MAGIC INNER JOIN ClientService cs ON cs.ClientServiceID = et.ClientServiceID 
# MAGIC LEFT OUTER JOIN LegalStatus ls ON ls.LegalStatusId = e.LegalStatusID
# MAGIC LEFT OUTER JOIN t_NAICS n ON n.industry = i.IndustryID
# MAGIC LEFT OUTER JOIN ( SELECT naics_code,max(date_begin) latest_date_begin 
# MAGIC               FROM t_client_SIC
# MAGIC             GROUP BY naics_code
# MAGIC           ) mn ON mn.naics_code = n.naics_code and mn.latest_date_begin = tcs.date_begin

# COMMAND ----------


sql="""
  SELECT DISTINCT e.ClientNumber
         , e.FiscalYearEnd	
         , e.EngagementName
         , e.DoNotDisclose	
         , et.EngagementTypeName	
         , tcs.naics_code
         , sce.Tax_form	
         , sce.Tax_Status	
         , sce.SEC	
         , '' Contact	--Empty
         , i.IndustryName
         , sc.SEC_id	
         , cs.Name	
         , ls.LegalStatusName
         , n.title

FROM Engagement e 
INNER JOIN EngagementType et ON et.EngagementTypeID = e.EngagementTypeID
INNER  JOIN t_client_SIC tcs ON tcs.client_number = e.ClientNumber  
INNER JOIN SourceClientExtended sce ON sce.ClientNumber = e.ClientNumber
INNER JOIN Industry i ON i.IndustryID = e.IndustryID
INNER JOIN SourceClient sc ON sc.SEC_id = sce.SEC_id and sc.ClientNumber = e.ClientNumber
INNER JOIN ClientService cs ON cs.ClientServiceID = et.ClientServiceID
LEFT OUTER JOIN LegalStatus ls ON ls.LegalStatusId = e.LegalStatusID
LEFT OUTER JOIN t_NAICS n ON n.industry = i.IndustryID
LEFT OUTER JOIN ( SELECT naics_code,max(date_begin) latest_date_begin 
              FROM t_client_SIC
            GROUP BY naics_code
          ) mn ON mn.naics_code = n.naics_code and mn.latest_date_begin = tcs.date_begin

""".format()
dest_df=(spark.sql(sql))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------

try:
  dest_df \
    .repartition(numPartitions) \
    .write \
    .mode("overwrite") \
    .option("MergeSchema",True) \
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
# MAGIC #### Create Databricks Table

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + schemaName.replace("-","")))

# COMMAND ----------

databaseTableName

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
  """.format(databaseTableName, enrichedPath)
  spark.sql(sql)

# COMMAND ----------

#sql="""OPTIMIZE {0}""".format(databaseTableName)
#spark.sql(sql)

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Purview Lineage

# COMMAND ----------

# sourceTables = {'t_client_SIC', 'ClientService','SourceClientExtended','Industry'}

# for srctbl in sourceTables:
# # create a list of the source tables, and execute the following for each table
#   sourceTable = srctbl #"Engagement"
#   print(sourceTable)

#   sourcePath = fullPathPrefix.replace("abfss://" + containerName + "@","https://") + "/rsm/Query/CurrentState/" + sourceTable + "/{SparkPartitions}"
#   destinationPath = enrichedPath.replace("abfss://" + containerName + "@","https://").replace("Query/Enriched/Tax", containerName + "/Query/Enriched/Tax") +  "/{SparkPartitions}"
#   print("Source Path {0}".format(sourcePath))
#   print("Destination Path {0}".format(destinationPath))
#   sourceAsset = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")
#   destinationAsset = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")

#   if sourceAsset != {} and destinationAsset != {}:
#     print('Create Purview Lineage')
#     create_purview_lineage_enrich(sourcePath, destinationPath, notebookName, str(-2043023456), str(notebookExecutionLogKey))
  

# COMMAND ----------

# sourceTables = {'Engagement','EngagementType','SourceClient','LegalStatus','t_NAICS'}

# for srctbl in sourceTables:
# # create a list of the source tables, and execute the following for each table
#   sourceTable = srctbl #"Engagement"
#   print(sourceTable)

#   sourcePath = fullPathPrefix.replace("abfss://" + containerName + "@","https://") + "/rsm/Query/CurrentState/" + sourceTable + "/{SparkPartitions}"
#   destinationPath = enrichedPath.replace("abfss://" + containerName + "@","https://").replace("Query/Enriched/Tax", containerName + "/Query/Enriched/Tax") +  "/{SparkPartitions}"
#   print("Source Path {0}".format(sourcePath))
#   print("Destination Path {0}".format(destinationPath))
#   sourceAsset = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")
#   destinationAsset = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")

#   if sourceAsset != {} and destinationAsset != {}:
#     print('Create Purview Lineage')
#     create_purview_lineage_enrich(sourcePath, destinationPath, notebookName, sourceTable, str(notebookExecutionLogKey))
  

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