DECLARE @ContainerExecutionGroupName VARCHAR(255) = 'RetailNext';
DECLARE @ContainerName varchar(255) = 'brtl';
DECLARE @DataFactoryName VARCHAR(255) = 'TBD';
DECLARE @NotebookName varchar(100) = 'RetailNext_%'

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

EXEC dbo.uspInsertNotebookParameters @NotebookName = 'RetailNext_Traffic_Ingest',@NotebookExecutionGroupName=@NotebookExecutionGroupName,@DataFactoryName=@DataFactoryName,@ContainerName=@ContainerName,@ContainerExecutionGroupName=@ContainerExecutionGroupName,@NotebookOrder=10,@NumPartitions=8,@QueryZoneSchemaName='brtl',@QueryZoneTableName='Traffic',@ExternalDataPath='',@RawDataPath='/Raw/RetailNext',@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Traffic',@PrimaryKeyColumns='',@TimeStampColumns='';

SELECT @NotebookExecutionGroupName = @ContainerExecutionGroupName+' Enrich';

EXEC dbo.uspInsertNotebookParameters @NotebookName = 'RetailNext_FCTTraffic_Enrich',@NotebookExecutionGroupName=@NotebookExecutionGroupName,@DataFactoryName=@DataFactoryName,@ContainerName=@ContainerName,@ContainerExecutionGroupName=@ContainerExecutionGroupName,@NotebookOrder=10,@NumPartitions=8,@QueryZoneSchemaName='brtl',@QueryZoneTableName='FCTTraffic',@ExternalDataPath='',@QueryZoneNotebookPath='/Append/Query Zone Processing - Append FCTTraffic',@PrimaryKeyColumns='',@TimeStampColumns='',@RawDataPath='';

SELECT  @NotebookExecutionGroupName = @ContainerExecutionGroupName+' Sanction';
