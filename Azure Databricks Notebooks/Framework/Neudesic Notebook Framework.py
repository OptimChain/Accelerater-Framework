# MAGIC  
# MAGIC %md
# MAGIC # Neudesic Notebook Framework
# MAGIC 
# MAGIC Clone this notebook to create a notebook that utilizes the Neudesic Databricks framework.  Replace this cell with documentation of what this notebook does. 
# MAGIC 
# MAGIC Updat the documentation sections below as follows.  
# MAGIC 
# MAGIC Provide a paragraph overview of the purpose of this notbook   
# MAGIC 
# MAGIC #### Usage
# MAGIC Give an example of how this notebook is intended to be used
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC List any requirements for using this notebook 
# MAGIC 
# MAGIC #### Details
# MAGIC Detailed documentation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")

# COMMAND ----------

#rename this when you clone/rename the notebook
notebookName = "Neudesic Notebook Framework"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Code Goes Here

# COMMAND ----------

# code goes here

# COMMAND ----------

#sourceName = "Test"
#errorCode = "779"
#errorDescription = "Testing error logging"
#log_event_notebook_error (notebookExecutionLogKey, sourceName, errorCode, errorDescription)

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