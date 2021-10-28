DELETE NotebookExecutionPlan
from NotebookExecutionPlan p
JOIN Notebook n on n.NotebookKey = p.NotebookKey
JOIN NotebookExecutionGroup g on g.NotebookExecutionGroupKey = p.NotebookExecutionGroupKey
AND NotebookExecutionGroupName Like 'CaseWare%'

DELETE NotebookExecutionPlan
from NotebookExecutionPlan p
JOIN Notebook n on n.NotebookKey = p.NotebookKey
JOIN NotebookExecutionGroup g on g.NotebookExecutionGroupKey = p.NotebookExecutionGroupKey
AND NotebookExecutionGroupName Like 'MAPS%'

DELETE NotebookExecutionPlan
from NotebookExecutionPlan p
JOIN Notebook n on n.NotebookKey = p.NotebookKey
JOIN NotebookExecutionGroup g on g.NotebookExecutionGroupKey = p.NotebookExecutionGroupKey
AND NotebookExecutionGroupName Like 'Tax%'

--CaseWare
--Convert
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Convert', @ContainerExecutionGroupName = 'CaseWare' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'CaseWare_Files_Combine'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = '' ,@FilePath = '', @ExternalDataPath = '/Landing/CaseWare/Dbf' ,@RawDataPath = '/Query/CurrentState/CaseWare' ,@RawZoneNotebookPath='/Framework/Raw Zone Processing - Landing to CurrentState Conversion dbf Combine',@QueryZoneNotebookPath='',@PrimaryKeyColumns = '';

--Enrich
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'CaseWare-Step1' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_EngagementProperties'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'EngagementProperties' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Enrich/Tax/Query Zone Processing - Overwrite EngagementProperties (FP)',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';

EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'CaseWare-Step2' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_AccountTrialBalance'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'AccountTrialBalance' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Enrich/Tax/Query Zone Processing - Overwrite AccountTrialBalance (AM)',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';

EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'CaseWare-Step3' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_TaxTrialBalance'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'TaxTrialBalance' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Enrich/Tax/Query Zone Processing - Overwrite TaxTrialBalance (BL)',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'CaseWare-Step3' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_AdjustingEntries'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'AdjustingEntries' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Enrich/Tax/Query Zone Processing - Overwrite AdjustingEntries (GL)',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';



--MAPS
--Ingest
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_Engagement'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'Engagement' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/Engagement' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_EngagementType'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'EngagementType' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/EngagementType' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_t_client_SIC'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 't_client_SIC' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/t_client_SIC' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_Industry'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'Industry' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/Industry' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_SourceClient'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'SourceClient' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/SourceClient' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_SourceClientExtended'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'SourceClientExtended' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/SourceClientExtended' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_Contact'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'Contact' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/Contact' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_ClientService'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'ClientService' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/ClientService' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_LegalStatus'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'LegalStatus' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/LegalStatus' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Ingest', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_t_NAICS'  ,@NotebookOrder = '10'  ,@NumPartitions = '16' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 't_NAICS' ,@FilePath = '' ,@RawDataPath = '/Raw/MAPS/t_NAICS' ,@QueryZoneNotebookPath='/Framework/Query Zone Processing - Overwrite Delta Lake',@PrimaryKeyColumns = '';

--Enrich
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Enrich', @ContainerExecutionGroupName = 'MAPS' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'MAPS_ClientInformation'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'ClientInformation' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '/Enrich/Tax/Query Zone Processing - Overwrite ClientInformation',@PrimaryKeyColumns = '' ,@TimeStampColumns = '';


--Tax

--Sanction
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Sanction', @ContainerExecutionGroupName = 'Tax' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_EngagementProperties'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'EngagementProperties' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '',@SanctionedZoneNotebookPath='/Framework/Sanctioned Zone Processing - Load Synapse DW';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Sanction', @ContainerExecutionGroupName = 'Tax' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_AccountTrialBalance'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'AccountTrialBalance' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '',@SanctionedZoneNotebookPath='/Framework/Sanctioned Zone Processing - Load Synapse DW';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Sanction', @ContainerExecutionGroupName = 'Tax' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_TaxTrialBalance'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'TaxTrialBalance' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '',@SanctionedZoneNotebookPath='/Framework/Sanctioned Zone Processing - Load Synapse DW';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Sanction', @ContainerExecutionGroupName = 'Tax' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_AdjustingEntries'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'AdjustingEntries' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '',@SanctionedZoneNotebookPath='/Framework/Sanctioned Zone Processing - Load Synapse DW';
EXECUTE [dbo].[uspInsertNotebookMetadata] @NotebookZone = 'Sanction', @ContainerExecutionGroupName = 'Tax' ,@ContainerName = 'rsm' ,@DataFactoryName = 'TBD' ,@NotebookName = 'Tax_ClientInformation'  ,@NotebookOrder = '10'  ,@NumPartitions = '8' ,@QueryZoneSchemaName = 'Tax' ,@QueryZoneTableName = 'ClientInformation' ,@ExternalDataPath = '' ,@RawDataPath = '' ,@QueryZoneNotebookPath = '',@SanctionedZoneNotebookPath='/Framework/Sanctioned Zone Processing - Load Synapse DW';
