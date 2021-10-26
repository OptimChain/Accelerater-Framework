# MAGIC  
# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

# set externalDataPath to the folder location containing the xlsx file
dbutils.widgets.text(name="externalDataPath", defaultValue="/Landing/SLC1A5_20200813_FFPE_tumor_vs_normal_Stomach_Cancer_TMAs/", label="External Data Path")

dbutils.widgets.text(name="containerName", defaultValue="seagen", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
containerName = dbutils.widgets.get("containerName")
mountSource = "abfss://"+containerName+"@"+adlsgen2storageaccountname+".dfs.core.windows.net/"

externalDataPath = dbutils.widgets.get("externalDataPath")

mountedPathPrefix = "/dbfs" 
mountedPath = "/mnt/"+containerName
mountedLandingPath =  mountedPath + externalDataPath


# COMMAND ----------

print("External Data Path: {0}".format(externalDataPath))

print("Mount Source: {0}".format(mountSource))
print("Mounted Path: {0}".format(mountedPath))
print("Mounted Landing Data Path: {0}".format(mountedLandingPath))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Script to Mount DataLake Storage to Databricks

# COMMAND ----------

#### Script to Mount Datalake container to Databrick if not mounted earlier
if not any(mount.mountPoint == mountedPath+"/" for mount in dbutils.fs.mounts()):
  print("mount it")
  configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": adlsclientid,
           "fs.azure.account.oauth2.client.secret": adlscredential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + adlstenantid + "/oauth2/token"}

  dbutils.fs.mount(
    source = mountSource,
    mount_point = mountedPath + "/",
    extra_configs = configs)
else:
  print("Already mounted")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read excel from mounted path

# COMMAND ----------

import pandas as pd

pd.read_excel('/dbfs/mnt/brtl/Landing/InventoryBudget//InventoryBudgetLevel3.xlsx')