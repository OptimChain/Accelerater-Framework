SET IDENTITY_INSERT dbo.CopyActivityDataType ON;

MERGE dbo.CopyActivityDataType AS t
  USING (
  VALUES (1,'Teradata Connector',1,Getdate(),Getdate(),Current_user),
   (2,'Azure Data Lake Store',1,Getdate(),Getdate(),Current_user),
   (3,'Azure SQL Data Warehouse',1,Getdate(),Getdate(),Current_user),
   (4,'Teradata Parallel Transporter',1,Getdate(),Getdate(),Current_user),
   (5,'File System',1,Getdate(),Getdate(),Current_user),
   (6,'Azure Blob Storage',1,Getdate(),Getdate(),Current_user),
   (7,'Azure File Storage',1,Getdate(),Getdate(),Current_user),
   (8,'SQL Server',1,Getdate(),Getdate(),Current_user),
   (9,'Google BigQuery',1,Getdate(),Getdate(),Current_user)
) as  s
		(   [CopyActivityDataTypeKey]
		   ,[CopyActivityDataTypeName]
           ,[IsActive]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser])
ON ( t.[CopyActivityDataTypeKey] = s.[CopyActivityDataTypeKey] )
WHEN MATCHED THEN 
	UPDATE SET   
		   [CopyActivityDataTypeName] = s.[CopyActivityDataTypeName]
           ,[IsActive] = s.[IsActive]
           ,[CreateDate] = s.[CreateDate]
           ,[ModifiedDate] = s.[ModifiedDate]
           ,[ModifiedUser] = s.[ModifiedUser]
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 [CopyActivityDataTypeKey]
		   ,[CopyActivityDataTypeName]
           ,[IsActive]
           ,[CreateDate]
           ,[ModifiedDate]
           ,[ModifiedUser]
		  )	
    VALUES(
			 s.[CopyActivityDataTypeKey]
		   ,s.[CopyActivityDataTypeName]
           ,s.[IsActive]
           ,s.[CreateDate]
           ,s.[ModifiedDate]
           ,s.[ModifiedUser]
		  );
GO

SET IDENTITY_INSERT dbo.CopyActivityDataType OFF;



