/*********************************************************************************************************************
	File: 	Notebook.data.sql

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
	09/18/2018	Alan Campbell				Added some additional stored procedures that are design to handle initial 
	                                        or incremental loads based on a passed parameter.
**********************************************************************************************************************/


SET IDENTITY_INSERT dbo.Notebook ON;

MERGE dbo.Notebook AS t
  USING (
  VALUES 

			(1,'Retail_Store',NULL,NULL,GetDate(),NULL),
			(2,'Retail_Product',NULL,NULL,GetDate(),NULL),
			(3,'Retail_Customer',NULL,NULL,GetDate(),NULL),
			(4,'Retail_Sales',NULL,NULL,GetDate(),NULL)	 
		 ) as s
		(
			 NotebookKey
			,NotebookName
			,NotebookCreationDate
			,NotebookCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.NotebookKey = s.NotebookKey )
WHEN MATCHED THEN 
	UPDATE SET   NotebookName = s.NotebookName
				,NotebookCreationDate = s.NotebookCreationDate
				,NotebookCreatedBy = s.NotebookCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 NotebookKey
			,NotebookName
			,NotebookCreationDate
			,NotebookCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.NotebookKey
			,s.NotebookName
			,s.NotebookCreationDate
			,s.NotebookCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.Notebook OFF;
