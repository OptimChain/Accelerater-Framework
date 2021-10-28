--Subset Processing 

--Remove Existing metadata
DELETE [dbo].[CopyActivityExecutionPlan]
  FROM [dbo].[CopyActivityExecutionPlan] p
  JOIN CopyActivityExecutionGroup g on p.CopyActivityExecutionGroupKey = g.CopyActivityExecutionGroupKey
  Where g.CopyActivityExecutionGroupName = 'AX Subset Copy Process'

DELETE NotebookExecutionPlan
FROM NotebookExecutionPlan p
JOIN Notebook n ON p.NotebookKey = n.NotebookKey
JOIN Container c ON p.ContainerKey = c.ContainerKey
JOIN NotebookExecutionGroup g on p.NotebookExecutionGroupKey = g.NotebookExecutionGroupKey
WHERE g.NotebookExecutionGroupName like 'AX Subset%'


--Copy Activity for Tables used in the final Query
EXECUTE [dbo].[uspInsertSQLScriptCopyMetadata] @ContainerName = 'brtl',@CopyActivityExecutionGroupName = 'AX Subset Copy Process',@CopyActivityDataName='AX_VENDPACKINGSLIPTRANS',@CopyActivityParameterValue='AXConnectionString',@CopyActivityDataSQLScript='SELECT * FROM dbo.VENDPACKINGSLIPTRANS WITH (NOLOCK)',@CopyActivityDataIncrementalSQLScript='SELECT * FROM dbo.VENDPACKINGSLIPTRANS WITH (NOLOCK) WHERE Cast(ACCOUNTINGDATE as Date)  >= Cast(DATEADD(DD,@IncrementalDays,GETDATE()) as Date)',@IsIncremental=0,@CopyActivityDataADLSFolderPath = '/Raw/AX/VENDPACKINGSLIPTRANS',@CopyActivityDataADLSFileName = 'VENDPACKINGSLIPTRANS.json';
EXECUTE [dbo].[uspInsertSQLScriptCopyMetadata] @ContainerName = 'brtl',@CopyActivityExecutionGroupName = 'AX Subset Copy Process',@CopyActivityDataName='AX_VENDPACKINGSLIPJOUR',@CopyActivityParameterValue='AXConnectionString',@CopyActivityDataSQLScript=' select * from dbo.VENDPACKINGSLIPJOUR WITH (NOLOCK)',@CopyActivityDataIncrementalSQLScript= ' select * from dbo.VENDPACKINGSLIPJOUR WITH (NOLOCK)',@IsIncremental=0,@CopyActivityDataADLSFolderPath = '/Raw/AX/VENDPACKINGSLIPJOUR',@CopyActivityDataADLSFileName ='VENDPACKINGSLIPJOUR.json'

--Ingest data from raw zone (Json in adls) to query zone / current state (delta lake in adls)
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'AX Subset' ,@ContainerName = 'brtl' ,@DataFactoryName = 'TBD' ,@NotebookName =   'AX_VENDPACKINGSLIPTRANS'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'brtl' ,@QueryZoneTableName = 'VENDPACKINGSLIPTRANS' ,@ExternalDataPath = '' ,@RawDataPath = '/Raw/AX/VENDPACKINGSLIPTRANS' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'AX Subset' ,@ContainerName = 'brtl' ,@DataFactoryName = 'TBD' ,@NotebookName =   'AX_VENDPACKINGSLIPJOUR'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'brtl' ,@QueryZoneTableName = 'VENDPACKINGSLIPJOUR' ,@ExternalDataPath = '' ,@RawDataPath = '/Raw/AX/VENDPACKINGSLIPJOUR' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';

-- Enrich to create symantic layer table to push to query zone / enrich and Synapse
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'AX Subset' ,@ContainerName = 'brtl' ,@DataFactoryName = 'TBD' ,@NotebookName = 'AX_FCTPurchaseLineReceipt'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'brtl' ,@QueryZoneTableName = 'FCTPurchaseLineReceipt' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Append/Query Zone Processing - Append FCTPurchaseLineReceipt',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';


	