


SET IDENTITY_INSERT  dbo.CopyActivityDataADLS ON;

MERGE dbo.CopyActivityDataADLS AS t
  USING (
   VALUES
-- AX
(100,'TSA_Activities','/Raw/AX/Activities','Activities.json',1,Getdate(),Getdate(),Getdate(),Current_user)

) as  s
		(   [CopyActivityDataADLSKey]
		   ,[CopyActivityDataADLSName]
           ,[CopyActivityDataADLSFolderPath]
           ,[CopyActivityDataADLSFileName]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser])
ON ( t.[CopyActivityDataADLSKey] = s.[CopyActivityDataADLSKey] )
WHEN MATCHED THEN 
	UPDATE SET   
		   [CopyActivityDataADLSName] = s.[CopyActivityDataADLSName]
           ,[CopyActivityDataADLSFolderPath] = s.[CopyActivityDataADLSFolderPath]
           ,[CopyActivityDataADLSFileName] = s.[CopyActivityDataADLSFileName]
           ,[IsActive] =s.[IsActive]
           ,[LastRunDate] = s.[LastRunDate]
           ,[CreateDate] = s.[CreateDate]
           ,[ModifiedDate] = s.[ModifiedDate]
           ,[ModifiedUser] = s.[ModifiedUser]
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			[CopyActivityDataADLSKey]
		   ,[CopyActivityDataADLSName]
           ,[CopyActivityDataADLSFolderPath]
           ,[CopyActivityDataADLSFileName]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser]
		  )	
    VALUES(
			 s.[CopyActivityDataADLSKey]
		   ,s.[CopyActivityDataADLSName]
           ,s.[CopyActivityDataADLSFolderPath]
           ,s.[CopyActivityDataADLSFileName]
           ,s.[IsActive]
           ,s.[LastRunDate]
           ,s.[CreateDate]
           ,s.[ModifiedDate]
           ,s.[ModifiedUser]
		  );
GO

SET IDENTITY_INSERT dbo.CopyActivityDataADLS OFF;