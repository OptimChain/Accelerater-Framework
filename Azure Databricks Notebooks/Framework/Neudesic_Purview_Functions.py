# MAGIC %md
# MAGIC 
# MAGIC # Neudesic_Purview_Functions
# MAGIC ###### Author:  Mike Sherrill 7/8/21
# MAGIC 
# MAGIC 
# MAGIC This notebook contains all the functions and utilities used by the Neudesic Framework.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ./Secrets-Databricks-Cache

# COMMAND ----------

dbutils.library.installPyPI("pyapacheatlas")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Purview Functions

# COMMAND ----------

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient

auth = ServicePrincipalAuthentication(
    tenant_id = adlstenantid, 
    client_id = adlsclientid, 
    client_secret = adlscredential
)

# Create a client to connect to your service.
client = PurviewClient(
    account_name = "hg-dev-analytics-scus-pa",
    authentication = auth
)

# COMMAND ----------

def create_purview_lineage_path(sourcePath, destinationPath, notebookName, tableName, notebookExecutionLogKey):
  from pyapacheatlas.auth import ServicePrincipalAuthentication
  from pyapacheatlas.core import PurviewClient
  from pyapacheatlas.core import AtlasProcess
  import json

  auth = ServicePrincipalAuthentication(
      tenant_id = adlstenantid, 
      client_id = adlsclientid, 
      client_secret = adlscredential
  )

  # Create a client to connect to your service.
  client = PurviewClient(
      account_name = "hg-dev-analytics-scus-pa",
      authentication = auth
  )
    
  sourceEntity = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_path")
  destinationEntity = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_path")
 
  process_qn = "notebook:  Purview - Create Lineage " + tableName
  process_type_name = "Process"

  newLineage = AtlasProcess(
    name = notebookName,
    typeName = process_type_name,
    qualified_name = process_qn,
    inputs = [sourceEntity["entities"][0]],
    outputs = [destinationEntity["entities"][0]],
    guid=hash(notebookName + tableName + notebookExecutionLogKey)
  )
             
  results = client.upload_entities(
    batch = [newLineage]
  )

  result=json.dumps(results, indent=2)
  
  return result

# COMMAND ----------

def create_purview_lineage_ingest(sourcePath, destinationPath, notebookName, tableName, notebookExecutionLogKey):
  from pyapacheatlas.auth import ServicePrincipalAuthentication
  from pyapacheatlas.core import PurviewClient
  from pyapacheatlas.core import AtlasProcess
  import json

  auth = ServicePrincipalAuthentication(
      tenant_id = adlstenantid, 
      client_id = adlsclientid, 
      client_secret = adlscredential
  )

  # Create a client to connect to your service.
  client = PurviewClient(
      account_name = "hg-dev-analytics-scus-pa",
      authentication = auth
  )
    
  sourceEntity = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_path")
  destinationEntity = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")
 
  process_qn = "notebook:  Purview - Create Lineage " + tableName
  process_type_name = "Process"

  newLineage = AtlasProcess(
    name = notebookName,
    typeName = process_type_name,
    qualified_name = process_qn,
    inputs = [sourceEntity["entities"][0]],
    outputs = [destinationEntity["entities"][0]],
    guid=hash(notebookName + tableName + notebookExecutionLogKey)
  )
             
  results = client.upload_entities(
    batch = [newLineage]
  )

  result=json.dumps(results, indent=2)
  
  return result

# COMMAND ----------

def create_purview_lineage_enrich(sourcePath, destinationPath, notebookName, tableName, notebookExecutionLogKey):
  from pyapacheatlas.auth import ServicePrincipalAuthentication
  from pyapacheatlas.core import PurviewClient
  from pyapacheatlas.core import AtlasProcess
  import json

  auth = ServicePrincipalAuthentication(
      tenant_id = adlstenantid, 
      client_id = adlsclientid, 
      client_secret = adlscredential
  )

  # Create a client to connect to your service.
  client = PurviewClient(
      account_name = "hg-dev-analytics-scus-pa",
      authentication = auth
  )
    
  sourceEntity = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")
  destinationEntity = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")
 
  process_qn = "notebook:  Purview - Create Lineage " + tableName
  process_type_name = "Process"

  newLineage = AtlasProcess(
    name = notebookName,
    typeName = process_type_name,
    qualified_name = process_qn,
    inputs = [sourceEntity["entities"][0]],
    outputs = [destinationEntity["entities"][0]],
    guid=hash(notebookName + tableName + notebookExecutionLogKey)
  )
             
  results = client.upload_entities(
    batch = [newLineage]
  )

  result=json.dumps(results, indent=2)
  
  return result

# COMMAND ----------

def create_purview_lineage_sanction(sourcePath, destinationPath, notebookName, tableName, notebookExecutionLogKey):
  from pyapacheatlas.auth import ServicePrincipalAuthentication
  from pyapacheatlas.core import PurviewClient
  from pyapacheatlas.core import AtlasProcess
  import json

  auth = ServicePrincipalAuthentication(
      tenant_id = adlstenantid, 
      client_id = adlsclientid, 
      client_secret = adlscredential
  )

  # Create a client to connect to your service.
  client = PurviewClient(
      account_name = "hg-dev-analytics-scus-pa",
      authentication = auth
  )
    
  sourceEntity = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")
  destinationEntity = client.get_entity(qualifiedName=destinationPath,typeName="azure_sql_dw_table") 
 
  process_qn = "notebook:  Purview - Create Lineage Sanction " + tableName
  process_type_name = "Process"

  newLineage = AtlasProcess(
    name = notebookName,
    typeName = process_type_name,
    qualified_name = process_qn,
    inputs = [sourceEntity["entities"][0]],
    outputs = [destinationEntity["entities"][0]],
    guid=hash(notebookName + tableName + notebookExecutionLogKey)
  )
             
  results = client.upload_entities(
    batch = [newLineage]
  )

  result=json.dumps(results, indent=2)
  
  return result