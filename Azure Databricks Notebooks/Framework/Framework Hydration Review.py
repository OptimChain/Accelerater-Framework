# MAGIC  
# MAGIC %md
# MAGIC # Framework Hydration Review
# MAGIC ###### Author: Eddie Edgeworth 4/12/18  
# MAGIC ###### Modifed for CLA: Mike Sherrill 6/20/19
# MAGIC 
# MAGIC #### Usage
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

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display All Container Notebook Execution Lists

# COMMAND ----------

ContainerNotebookExecutionLists = get_all_container_notebook_execution_lists()
display(ContainerNotebookExecutionLists)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display All Notebook Execution Lists

# COMMAND ----------

NotebookExecutionLists = get_all_notebook_execution_lists()
display(NotebookExecutionLists)

# COMMAND ----------

NotebookParameters = get_notebook_parameters_for_all_notebooks()
display(NotebookParameters)

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 