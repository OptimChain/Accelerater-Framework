/*********************************************************************************************************************
	File: 	ContainerExecutionGroup.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	08/07/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	08/07/2018	Joseph Barth				Created.			
	06/11/2019	Mike Sherrill				Modified for CLA Containers
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.ContainerExecutionGroup ON;

MERGE dbo.ContainerExecutionGroup AS t
  USING (
  VALUES 
		(1,'TSA Ingest',1,1,0,getdate(),NULL),
		(2,'TSA Enrich',1,1,0,getdate(),NULL),
		(3,'TSA Sanction',1,1,0,getdate(),NULL)
				) as s
		(
			 ContainerExecutionGroupKey
			,ContainerExecutionGroupName
			,ContainerKey
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		)
ON ( t.ContainerExecutionGroupKey = s.ContainerExecutionGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   ContainerExecutionGroupName = s.ContainerExecutionGroupName
				,ContainerKey = s.ContainerKey
				,IsActive = s.IsActive
				,IsEmailEnabled = s.IsEmailEnabled
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 ContainerExecutionGroupKey
			,ContainerExecutionGroupName
			,ContainerKey
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.ContainerExecutionGroupKey
			,s.ContainerExecutionGroupName
			,s.ContainerKey
			,s.IsActive
			,s.IsEmailEnabled
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.ContainerExecutionGroup OFF;