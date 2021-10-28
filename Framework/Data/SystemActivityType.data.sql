/*********************************************************************************************************************
	File: 	SystemActivityType.data.sql

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

SET IDENTITY_INSERT dbo.SystemActivityType ON;

MERGE dbo.SystemActivityType AS t
  USING (
  VALUES 
		 (1,'CopyActivity',GetDate(),NULL)
		,(2,'PipeLine',GetDate(),NULL)
		,(3,'NoteBook',GetDate(),NULL)
		,(4,'StoredProcedure',GetDate(),NULL)
		,(5,'DataFlow',GetDate(),NULL)
		
		) as s
		(
			 SystemActivityTypeKey
			,SystemActivityTypeName
			,CreatedDate
			,ModifiedDate
		)
ON ( t.SystemActivityTypeKey = s.SystemActivityTypeKey )
WHEN MATCHED THEN 
	UPDATE SET   SystemActivityTypeName = s.SystemActivityTypeName
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 SystemActivityTypeKey
			,SystemActivityTypeName
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.SystemActivityTypeKey
			,s.SystemActivityTypeName
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.SystemActivityType OFF;


