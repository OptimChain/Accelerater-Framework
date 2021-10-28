DECLARE @ContainerName varchar(30) = 'client000000001'

DECLARE @NotebookExecutionGroup varchar(50) = @ContainerName + ' Convert'
Exec uspGetNotebookExecutionList @NotebookExecutionGroup

SELECT @NotebookExecutionGroup = @ContainerName + ' Ingest'
Exec uspGetNotebookExecutionList @NotebookExecutionGroup

SELECT @NotebookExecutionGroup = @ContainerName + ' Enrich and Publish'
Exec uspGetNotebookExecutionList @NotebookExecutionGroup

SELECT @NotebookExecutionGroup = @ContainerName + ' Sanction and Publish'
Exec uspGetNotebookExecutionList @NotebookExecutionGroup
