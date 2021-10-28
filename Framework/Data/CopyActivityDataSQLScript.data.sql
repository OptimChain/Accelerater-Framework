SET IDENTITY_INSERT dbo.CopyActivityDataSQLScript ON;

MERGE dbo.CopyActivityDataSQLScript AS t
  USING (

VALUES
(100,'TSA_Activities','SELECT * FROM dbo.Activities WITH (NOLOCK)','SELECT * FROM dbo.Activities WITH (NOLOCK) ',0,1,Getdate(),Getdate(),Getdate(),Current_user)
  		) as  s
		(   [CopyActivityDataSQLScriptKey],
			[CopyActivityDataSQLScriptName],
			[CopyActivityDataSqlScript],
			[CopyActivityDataIncrementalSqlScript],
			[IsIncrementalLoad],
			[IsActive],
			[LastRunDate],
			[CreateDate],
			[ModifiedDate],
			[ModifiedUser]  )
ON ( t.[CopyActivityDataSQLScriptKey] = s.[CopyActivityDataSQLScriptKey] )
WHEN MATCHED THEN 
	UPDATE SET   
		   [CopyActivityDataSQLScriptName] =s.[CopyActivityDataSQLScriptName],
			[CopyActivityDataSqlScript] = s.[CopyActivityDataSqlScript],
			[CopyActivityDataIncrementalSqlScript] = s.[CopyActivityDataIncrementalSqlScript],
			[IsIncrementalLoad] = s.[IsIncrementalLoad],
			[IsActive] = s.[IsActive],
			[LastRunDate] = s.[LastRunDate],
			[CreateDate] = s.[CreateDate],
			[ModifiedDate] = s.[ModifiedDate],
			[ModifiedUser] = s.[ModifiedUser]
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			[CopyActivityDataSQLScriptKey],
			[CopyActivityDataSQLScriptName],
			[CopyActivityDataSqlScript],
			[CopyActivityDataIncrementalSqlScript],
			[IsIncrementalLoad],			
			[IsActive],
			[LastRunDate],
			[CreateDate],
			[ModifiedDate],
			[ModifiedUser]
		  )	
    VALUES(
			 s.[CopyActivityDataSQLScriptKey],
			s.[CopyActivityDataSQLScriptName],
			s.[CopyActivityDataSqlScript],
			s.[CopyActivityDataIncrementalSqlScript],
			s.[IsIncrementalLoad],
			s.[IsActive],
			s.[LastRunDate],
			s.[CreateDate],
			s.[ModifiedDate],
			s.[ModifiedUser]
		  );
GO

SET IDENTITY_INSERT dbo.CopyActivityDataSQLScript OFF;