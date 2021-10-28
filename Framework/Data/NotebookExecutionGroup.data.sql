/*********************************************************************************************************************
	File: 	NotebookExecutionGroup.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	08/16/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	08/16/2018	Joseph Barth				Created.	
	09/18/2018	Alan Campbell				Added some additional groups to align better with other Framework examples.
	05/14/2020  Mike Sherrill				Updated for Bartell Drugs
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.NotebookExecutionGroup ON;

MERGE dbo.NotebookExecutionGroup AS t
  USING (
  VALUES 
			 (1,'AX Ingest',1,1,GetDate(),NULL),
			 (2,'AX Enrich',1,1,GetDate(),NULL),
			 (3,'AX Sanction',1,1,GetDate(),NULL)

		) as s
		(
			 NotebookExecutionGroupKey
			,NotebookExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		)
ON ( t.NotebookExecutionGroupKey = s.NotebookExecutionGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   NotebookExecutionGroupName = s.NotebookExecutionGroupName
				,IsActive = s.IsActive
				,IsEmailEnabled = s.IsEmailEnabled
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 NotebookExecutionGroupKey
			,NotebookExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.NotebookExecutionGroupKey
			,s.NotebookExecutionGroupName
			,s.IsActive
			,s.IsEmailEnabled
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.NotebookExecutionGroup OFF;