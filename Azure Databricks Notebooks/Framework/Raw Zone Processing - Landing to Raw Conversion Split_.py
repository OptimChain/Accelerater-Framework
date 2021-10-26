# MAGIC  
# MAGIC %md
# MAGIC # Raw Zone Processing - Landing to Raw Conversion Split_
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

#dbutils.widgets.removeAll()
#dbutils.widgets.remove('yearMonth')

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client0000001", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="externalDataPath", defaultValue="/Landing/DatasetByMonth", label="External Data Path")
dbutils.widgets.text(name="rawDataPath", defaultValue="/Raw", label="Raw Data Path")
dbutils.widgets.text(name="splitChar", defaultValue="_", label="Split Character")
parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
mountSource = "abfss://"+containerName+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/"

externalDataPath = dbutils.widgets.get("externalDataPath")
rawDataPath = dbutils.widgets.get("rawDataPath")
splitChar = dbutils.widgets.get("splitChar")
mountedPathPrefix = "/dbfs" 
mountedPath = "/mnt/"+containerName
mountedLandingPath =  mountedPath + externalDataPath
mountedRawPath = mountedPath + rawDataPath


# COMMAND ----------

notebookName = "Raw Zone Processing - Landing to Raw Conversion "
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

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

#dbutils.fs.unmount(mountedPath+"/")
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
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/4aaa468e-93ba-4ee3-ab9f-6a247aa3ade0/oauth2/token"}

  dbutils.fs.mount(
    source = mountSource,
    mount_point = mountedPath + "/",
    extra_configs = configs)
else:
  print("Already mounted")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Script to Convert xlsx and csv in Landing to json at Raw

# COMMAND ----------

import os
import json
import pandas as pd

def records_for_json(df):
    columns = [str(k) for k in df.columns]
    return [dict(zip(columns, row)) for row in df.values]

try:
  #Function to convert xlsx to JSON
  def convertExcelToJson(file,destinationFileName):

      class PandasJsonEncoder(json.JSONEncoder):
        def default(self,obj):
          import datetime
          if any(isinstance(obj, cls) for cls in (datetime.time, datetime.datetime, pd.Timestamp)):
            return obj.__str__()
          elif pd.isnull(obj):
            return None
          else:
            return super(PandasJsonEncoder, self).default(obj)

      dictDataList =[]
      sheetName = pd.ExcelFile(file).sheet_names[0]
      #print(sheetName)
      sheet = pd.read_excel(file, sheet_name=sheetName)
      records = records_for_json(sheet)
      #print(records)
      for record in records:
        dictDataList.append(json.dumps(record,cls=PandasJsonEncoder) + "\n")

      dictDataStr = ''.join(dictDataList)
      # Write to file
      with open(destinationFileName, 'w') as f:
        f.write(dictDataStr)
      
except Exception as e:
  sourceName = "Raw Zone Processing - XLSX to json conversion"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)
  
  
try:
  
  #function to convert csv to json
  def convertCsvToJson(filepath,destinationFileName):
    import csv
    dictDatacsvList =[]
    with open(filepath, encoding = "ISO-8859-1") as csvfile:
      csvReader = csv.DictReader(csvfile)
      rows = list(csvReader)
    for row in rows:
      dictDatacsvList.append(json.dumps(row) + "\n")

    dictDatacsvStr = ''.join(dictDatacsvList)
    # Write to file
    with open(destinationFileName, 'w') as f:
      f.write(dictDatacsvStr)

except Exception as e:
  sourceName = "Raw Zone Processing - CSV to json conversion"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)
    
def fileConversion(sourceFile,destinationFile,fileExtension):
  if fileExtension == 'xlsx':
    print("found xlsx")
    convertExcelToJson(sourceFilePath, destinationFilePath)
    print(sourceFilePath)
    print(destinationFilePath)
  elif fileExtension == 'csv':
    print("found csv")
    convertCsvToJson(sourceFilePath, destinationFilePath)
    print(sourceFilePath)
    print(destinationFilePath)
  else:
    print("No Valid file format to convert")
  
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
        if splitChar != '':
          splitfileName = "/" + fileName.split(splitChar,1)[1]
          splitDateFolderName = "/" + fileName.split(splitChar,1)[0]
          createDir = dirName.replace(mountedPathPrefix,'',1).replace("/Landing" , "/Raw" , 1) + splitfileName
          dbutils.fs.mkdirs(createDir)
          destinationFilePath = mountedPathPrefix+createDir + splitfileName +'.json'
        else:
          destinationFilePath = mountedPathPrefix+createDir +"/"+ fileName +'.json'
        sourceFilePath = dirName+"/"+fname
        fileConversion(sourceFilePath,destinationFilePath,fileExtension)
      
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