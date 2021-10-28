

/*********************************************************************************************************************
	File: 	NotebookParameter.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	09/20/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	09/20/2018	Alan Campbell				Created.
	
**********************************************************************************************************************/



MERGE dbo.NotebookParameter AS t
  USING (
  VALUES 
			(1,'schemaName','Retail',GetDate(),NULL)
			,(1,'tableName','Store',GetDate(),NULL)
			,(1,'loadStrategy','Replace',GetDate(),NULL)
			,(1,'numPartitions','8',GetDate(),NULL)
		  
			,(2,'schemaName','Retail',GetDate(),NULL)
			,(2,'tableName','Product',GetDate(),NULL)
			,(2,'loadStrategy','Replace',GetDate(),NULL)
			,(2,'numPartitions','8',GetDate(),NULL)
		  
			,(3,'schemaName','Retail',GetDate(),NULL)
			,(3,'tableName','Customer',GetDate(),NULL)
			,(3,'loadStrategy','Replace',GetDate(),NULL)
			,(3,'numPartitions','8',GetDate(),NULL)
		  
			,(4,'schemaName','Retail',GetDate(),NULL)
			,(4,'tableName','Order',GetDate(),NULL)
			,(4,'loadStrategy','Replace',GetDate(),NULL)
			,(4,'numPartitions','8',GetDate(),NULL)

		  ) as s
		(
			 NotebookKey
			,NotebookParameterName
			,NotebookParameterValue
			,CreatedDate
			,ModifiedDate
		)
ON ( t.NotebookKey = s.NotebookKey AND t.NotebookParameterName = s.NotebookParameterName )
WHEN MATCHED THEN 
	UPDATE SET   NotebookParameterValue = s.NotebookParameterValue
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 NotebookKey
			,NotebookParameterName
			,NotebookParameterValue
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.NotebookKey
			,s.NotebookParameterName
			,s.NotebookParameterValue
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

