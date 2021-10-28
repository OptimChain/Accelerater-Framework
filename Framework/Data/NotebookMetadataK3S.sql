DECLARE @ContainerExecutionGroupName VARCHAR(255) = 'K3S';
DECLARE @ContainerName varchar(255) = 'brtl';
DECLARE @DataFactoryName VARCHAR(255) = 'TBD';
DECLARE @NotebookName varchar(100) = 'K3S_%'

--DELETE NotebookExecutionPlan
DELETE NotebookExecutionPlan
FROM NotebookExecutionPlan p
JOIN Notebook n ON p.NotebookKey = n.NotebookKey
JOIN Container c ON p.ContainerKey = c.ContainerKey
WHERE c.ContainerName = @ContainerName AND n.NotebookName like @NotebookName

--DELETE NotebookParameter
DELETE NotebookParameter
FROM NotebookParameter p
JOIN Notebook n ON p.NotebookKey = n.NotebookKey
WHERE NotebookName like @ContainerName+'%' AND n.NotebookName like @NotebookName


DECLARE @NotebookExecutionGroupName varchar(255) = @ContainerExecutionGroupName+' Convert'
-- Use Convert notebooks for .xlsx or .GZip files

SELECT @NotebookExecutionGroupName = @ContainerExecutionGroupName+' Ingest';
-- before running ingest it is necessary to run ADF pipeline that copies the extracted .txt files to .json in raw

EXEC dbo.uspInsertNotebookParameters @NotebookName = 'K3S_AssistedOrders_Ingest',@NotebookExecutionGroupName=@NotebookExecutionGroupName,@DataFactoryName=@DataFactoryName,@ContainerName=@ContainerName,@ContainerExecutionGroupName=@ContainerExecutionGroupName,@NotebookOrder=10,@NumPartitions=8,@QueryZoneSchemaName='brtl',@QueryZoneTableName='AssistedOrders',@ExternalDataPath='',@RawDataPath='/Raw/K3S/AssistedOrders',@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns='',@TimeStampColumns='#addFileName';

SELECT @NotebookExecutionGroupName = @ContainerExecutionGroupName+' Enrich';

EXEC dbo.uspInsertNotebookParameters @NotebookName = 'K3S_AssistedOrders_Enrich',@NotebookExecutionGroupName=@NotebookExecutionGroupName,@DataFactoryName=@DataFactoryName,@ContainerName=@ContainerName,@ContainerExecutionGroupName=@ContainerExecutionGroupName,@NotebookOrder=10,@NumPartitions=32,@QueryZoneSchemaName='brtl',@QueryZoneTableName='AssistedOrders',@ExternalDataPath='',@QueryZoneNotebookPath='/Append/Query Zone Processing - Append FCTAssistedOrders',@PrimaryKeyColumns='',@TimeStampColumns='',@RawDataPath='';

SELECT  @NotebookExecutionGroupName = @ContainerExecutionGroupName+' Sanction';

