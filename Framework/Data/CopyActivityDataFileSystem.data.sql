SET IDENTITY_INSERT dbo.CopyActivityDataFileSystem ON;

MERGE dbo.CopyActivityDataFileSystem AS t
  USING (
  VALUES 
(200,'K3S_AssistedOrders','//fv-bd-vfs01p/Integrations/K3SImport/Archive/','STR*',1,Getdate(),Getdate(),Getdate(),Current_user)
) as  s
		(   [CopyActivityDataFileSystemKey]
		   ,[CopyActivityDataFileSystemName]
           ,[CopyActivityDataFileSystemFolderPath]
           ,[CopyActivityDataFileSystemFileName]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser])
ON ( t.[CopyActivityDataFileSystemKey] = s.[CopyActivityDataFileSystemKey] )
WHEN MATCHED THEN 
	UPDATE SET   
		   [CopyActivityDataFileSystemName] = s.[CopyActivityDataFileSystemName]
           ,[CopyActivityDataFileSystemFolderPath] = s.[CopyActivityDataFileSystemFolderPath]
           ,[CopyActivityDataFileSystemFileName] = s.[CopyActivityDataFileSystemFileName]
           ,[IsActive] = s.[IsActive]
           ,[LastRunDate] = s.[LastRunDate]
           ,[CreateDate] = s.[CreateDate]
           ,[ModifiedDate] = s.[ModifiedDate]
           ,[ModifiedUser] = s.[ModifiedUser]
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			[CopyActivityDataFileSystemKey]
		   ,[CopyActivityDataFileSystemName]
           ,[CopyActivityDataFileSystemFolderPath]
           ,[CopyActivityDataFileSystemFileName]
           ,[IsActive]
           ,[LastRunDate]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser]
		  )	
    VALUES(
			 s.[CopyActivityDataFileSystemKey]
		   ,s.[CopyActivityDataFileSystemName]
           ,s.[CopyActivityDataFileSystemFolderPath]
           ,s.[CopyActivityDataFileSystemFileName]
           ,s.[IsActive]
           ,s.[LastRunDate]
           ,s.[CreateDate]
           ,s.[ModifiedDate]
           ,s.[ModifiedUser]
		  );
GO

SET IDENTITY_INSERT dbo.CopyActivityDataFileSystem OFF;