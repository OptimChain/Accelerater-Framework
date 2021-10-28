
SET IDENTITY_INSERT dbo.CopyActivitySink ON;

MERGE dbo.CopyActivitySink AS t
  USING (
  --AX

SELECT 100, 'TSA_Activities',cadf.[CopyActivityDataSQLScriptKey],cadsts.[CopyActivityDataTypeKey],cadsadls.[CopyActivityDataADLSKey],cadstadls.[CopyActivityDataTypeKey],1,getdate(),getdate(),getdate(),CURRENT_USER FROM [dbo].[CopyActivityDataSQLScript] cadf, [dbo].[CopyActivityDataType] cadsts,[dbo].[CopyActivityDataADLS] cadsadls,[dbo].[CopyActivityDataType] cadstadls where cadf.[CopyActivityDataSQLScriptName] = 'TSA_Activities' and cadf.[IsActive] = 1  and cadsts.[CopyActivityDataTypeName] = 'SQL Server' and cadsadls.[CopyActivityDataADLSName] = 'TSA_Activities' and cadsadls.[IsActive] = 1 and cadstadls.[CopyActivityDataTypeName] = 'Azure Data Lake Store' 
) as  s
		(   [CopyActivitySinkKey]
		   ,[CopyActivitySinkName]
           ,[CopyActivitySinkDataKey]
           ,[CopyActivitySinkDataTypeKey]
           ,[CopyActivitySinkDataTargetKey]
           ,[CopyActivitySinkDataTargetTypeKey]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser])
ON ( t.[CopyActivitySinkKey] = s.[CopyActivitySinkKey] )
WHEN MATCHED THEN 
	UPDATE SET   
			[CopyActivitySinkName] = s.[CopyActivitySinkName]
           ,[CopyActivitySinkDataKey] = s.[CopyActivitySinkDataKey]
           ,[CopyActivitySinkDataTypeKey] = s.[CopyActivitySinkDataTypeKey]
           ,[CopyActivitySinkDataTargetKey] = s.[CopyActivitySinkDataTargetKey]
           ,[CopyActivitySinkDataTargetTypeKey] = s.[CopyActivitySinkDataTargetTypeKey]
           ,[IsActive] = s.[IsActive]
           ,[LastRunDate] = s.[LastRunDate]
           ,[CreateDate] = s.[CreateDate]
           ,[ModifiedDate] = s.[ModifiedDate]
           ,[ModifiedUser] = s.[ModifiedUser]
WHEN NOT MATCHED BY TARGET THEN
    INSERT( [CopyActivitySinkKey]
		   ,[CopyActivitySinkName]
           ,[CopyActivitySinkDataKey]
           ,[CopyActivitySinkDataTypeKey]
           ,[CopyActivitySinkDataTargetKey]
           ,[CopyActivitySinkDataTargetTypeKey]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser]
		  )	
    VALUES(
		   [CopyActivitySinkKey]
		   ,[CopyActivitySinkName]
           ,[CopyActivitySinkDataKey]
           ,[CopyActivitySinkDataTypeKey]
           ,[CopyActivitySinkDataTargetKey]
           ,[CopyActivitySinkDataTargetTypeKey]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser]
		  );
GO

SET IDENTITY_INSERT dbo.CopyActivitySink OFF;