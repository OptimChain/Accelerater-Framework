# MAGIC  
# MAGIC %md
# MAGIC # Raw Zone Processing - Landing to Raw Conversion
# MAGIC ###### Author: Rohit Srivastava 7/10/19
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
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

dbutils.widgets.text(name="containerName", defaultValue="rsm", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="externalDataPath", defaultValue="/Landing/CaseWare/TrialBalance", label="External Data Path")
dbutils.widgets.text(name="rawDataPath", defaultValue="/Raw", label="Raw Data Path")
parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
mountSource = "abfss://"+containerName+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/"

externalDataPath = dbutils.widgets.get("externalDataPath")
rawDataPath = dbutils.widgets.get("rawDataPath")
mountedPathPrefix = "/dbfs" 
mountedPath = "/mnt/"+containerName
mountedLandingPath =  mountedPath + externalDataPath
mountedRawPath = mountedPath + rawDataPath
sourcePath = "https://" + adlsgen2storageaccountname + ".dfs.core.windows.net/" + containerName + externalDataPath + "/"
destinationPath = "https://" + adlsgen2storageaccountname + ".dfs.core.windows.net/" + containerName + "/Query/CurrentState/CaseWare/"  

# COMMAND ----------

print("External Data Path: {0}".format(externalDataPath))
print("Raw Data Path: {0}".format(rawDataPath))
print("Mount Source: {0}".format(mountSource))
print("Mounted Path: {0}".format(mountedPath))
print("Mounted Landing Data Path: {0}".format(mountedLandingPath))
print("Mounted Raw Data Path: {0}".format(mountedRawPath))

# COMMAND ----------

destinationPath

# COMMAND ----------

#Landing/Dbf

sourceEntity = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_path")

# COMMAND ----------

#CurrentState/CaseWare

destinationEntity = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_path")

# COMMAND ----------

sourceEntity

# COMMAND ----------

destinationEntity

# COMMAND ----------

notebookName = "Raw Zone Processing - Landing to Raw Conversion Dbf"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

#Create Lineage using function

create_purview_lineage_path(sourcePath, destinationPath, notebookName,"",str(notebookExecutionLogKey))

# COMMAND ----------

print("External Data Path: {0}".format(externalDataPath))
print("Raw Data Path: {0}".format(rawDataPath))
print("Mount Source: {0}".format(mountSource))
print("Mounted Path: {0}".format(mountedPath))
print("Mounted Landing Data Path: {0}".format(mountedLandingPath))
print("Mounted Raw Data Path: {0}".format(mountedRawPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Script to Mount DataLake Storage to Databrick

# COMMAND ----------

#dbutils.fs.unmount(mountedLandingPath+"/")
#containerName
#dbutils.fs.mounts()

# COMMAND ----------

#### Script to Mount Datalake container to Databrick if not mounted earlier
if not any(mount.mountPoint == mountedPath+"/" for mount in dbutils.fs.mounts()):
  print("mount it")
  configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": adlsclientid,
           "fs.azure.account.oauth2.client.secret": adlscredential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1e3e71be-fcca-4284-9031-688cc8f37b6b/oauth2/token"}

  dbutils.fs.mount(
    source = mountSource,
    mount_point = mountedPath + "/",
    extra_configs = configs)
else:
  print("Already mounted")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Script to Convert dbf in Landing to CurrentState

# COMMAND ----------

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)

# COMMAND ----------

import os
import json
import pandas as pd
from dbfread import DBF
from pyspark.sql.functions import col, lit

def records_for_json(df):
    columns = [str(k) for k in df.columns]
    return [dict(zip(columns, row)) for row in df.values]
  
def rmissingvaluecol(dff,threshold):
    l = []
    l = list(dff.drop(dff.loc[:,list((100*(dff.isnull().sum()/len(dff.index))>=threshold))].columns, 1).columns.values)
    print("# Columns having more than %s percent missing values:"%threshold,(dff.shape[1] - len(l)))
    print("Columns:\n",list(set(list((dff.columns.values))) - set(l)))
    return l

try:
  
  def fileConversion(sourceFile,destinationFile):
    
    if fileExtension == 'dbf':
      print("Found dbf file")
      print(sourceFile)
      # convertDbfToJson(sourceFilePath, destinationFilePath)
      pdf=DBF(sourceFile, load=True, ignore_missing_memofile=True,encoding='iso-8859-1')
                
      dest_df  = pd.DataFrame(iter(pdf)) #Convert to DataFrame
      
      #dest_df = dest_df.drop(columns=['EXINFO'])
      l = rmissingvaluecol(dest_df,100)
      print(len(l))
      if len(l) > 0:
        dest_df = dest_df[l]
        
      dest_df = dest_df.astype(str) 
      #display(dest_df)
      if dest_df.empty == False:
        dest_df = spark.createDataFrame(dest_df)
        sourceFile = sourceFile.replace('.dbf','')
        sourceFile = sourceFile.replace('/dbfs/mnt/rsm/Landing/Dbf/','')
        dest_df2 = dest_df.withColumn("FILENAME", lit(sourceFile))
        #display(dest_df2)
        dest_df2 \
          .write \
          .mode("overwrite") \
          .option("mergeSchema",True) \
          .format("delta") \
          .save(destinationFilePath)

    else:
      print("No Valid file format to convert")

except Exception as e:
  sourceName = "Raw Zone Processing - dbf to Delta conversion"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)
        

try:
  for dirName, subdirList, fileList in os.walk(mountedPathPrefix+mountedLandingPath, topdown=False):
    if len(fileList)>0:
      #print('Found directory: %s' % dirName)
      createDir = dirName.replace(mountedPathPrefix,'',1).replace("/Landing" , "/Raw" , 1)
      dbutils.fs.mkdirs(createDir)
      for fname in fileList:
        #print('\t%s' % fname)
        fileName = fname.split('.')[0]
        fileExtension = fname.split('.')[1]
        sourceFilePath = dirName+"/"+fname
        lasttwochars = fileName[-2:]
        fileName = fileName[:-2]
        destinationFilePath = mountSource + rawDataPath + "/" + fileName + "/" + lasttwochars #mountedPathPrefix+createDir +"/"+ fileName +'.json'
#        destinationFilePath = mountSource + rawDataPath + "/" + lasttwochars #mountedPathPrefix+createDir +"/"+ fileName +'.json'
#        print(destinationFilePath)
        fileConversion(sourceFilePath,destinationFilePath)
      
except Exception as e:
  sourceName = "Raw Zone Processing - Batch: Read Data from Raw Zone"
  errorCode = "200"
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