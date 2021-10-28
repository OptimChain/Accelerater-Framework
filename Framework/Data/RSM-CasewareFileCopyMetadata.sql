
DELETE [CopyActivityExecutionPlan]
   FROM [dbo].[CopyActivityExecutionPlan] p
  JOIN CopyActivitySink s
  on p.CopyActivitySinkKey = s.CopyActivitySinkKey
  JOIN CopyActivityExecutionGroup g on p.CopyActivityExecutionGroupKey = g.CopyActivityExecutionGroupKey
  where CopyActivityExecutionGroupName like
  'CaseWare Copy Process'

  exec dbo.uspInsertFileSystemCopyMetadata @ContainerName = 'rsm', @CopyActivityExecutionGroupName = 'CaseWare Copy Process' ,@CopyActivityDataName = 'CaseWare_Dbf', @CopyActivityParameterValue = 'CaseWareConnectionString', @CopyActivityDataFileSystemFolderPath = 'c:/CaseWare/', @CopyActivityDataFileSystemFileName = '*.dbf', @CopyActivityDataADLSFolderPath = '/Landing/CaseWare/Dbf', @CopyActivityDataADLSFileName = ''
  exec dbo.uspInsertFileSystemCopyMetadata @ContainerName = 'rsm', @CopyActivityExecutionGroupName = 'CaseWare Copy Process' ,@CopyActivityDataName = 'CaseWare_fpt', @CopyActivityParameterValue = 'CaseWareConnectionString', @CopyActivityDataFileSystemFolderPath = 'c:/CaseWare/', @CopyActivityDataFileSystemFileName = '*.fpt', @CopyActivityDataADLSFolderPath = '/Landing/CaseWare/Dbf', @CopyActivityDataADLSFileName = ''
