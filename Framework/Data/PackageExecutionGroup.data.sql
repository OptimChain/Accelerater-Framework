/*********************************************************************************************************************
	File: 	PackageExecutionGroup.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	03/16/2016

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	03/16/2016	Alan Campbell				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.PackageExecutionGroup ON;

MERGE dbo.PackageExecutionGroup AS t
  USING (
  VALUES 
		
		 (1,'Master',1,1,GetDate(),NULL)
		,(2,'Staging A',1,0,GetDate(),NULL)
		,(3,'Staging B',1,0,GetDate(),NULL)
		,(4,'Staging C',1,0,GetDate(),NULL)
		,(5,'Dimension A',1,0,GetDate(),NULL)
		,(6,'Dimension B',1,0,GetDate(),NULL)
		,(7,'Fact A',1,0,GetDate(),NULL)
		,(8,'Fact B',1,0,GetDate(),NULL)
		,(9,'Tabular',1,0,GetDate(),NULL)		
		,(10,'Test',1,0,GetDate(),NULL)
		,(11, 'Maintenance',0,0,GetDate(),NULL)
		,(12, 'Subscriptions',1,0,GetDate(),NULL)
		
		) as s
		(
			 PackageExecutionGroupKey
			,PackageExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PackageExecutionGroupKey = s.PackageExecutionGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   PackageExecutionGroupName = s.PackageExecutionGroupName
				,IsActive = s.IsActive
				,IsEmailEnabled = s.IsEmailEnabled
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PackageExecutionGroupKey
			,PackageExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PackageExecutionGroupKey
			,s.PackageExecutionGroupName
			,s.IsActive
			,s.IsEmailEnabled
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.PackageExecutionGroup OFF;