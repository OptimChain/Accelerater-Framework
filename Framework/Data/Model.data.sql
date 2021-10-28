/*********************************************************************************************************************
	File: 	Model.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	2/14/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	2/14/2018	Alan Campbell				Created.			
**********************************************************************************************************************/

MERGE dbo.Model AS t
  USING (
  VALUES 
		 (1,'Retail Analysis',GetDate(),NULL)
		
		) as s
		(
			 ModelKey
			,ModelName
			,CreatedDate
			,ModifiedDate
		)
ON ( t.ModelKey = s.ModelKey )
WHEN MATCHED THEN 
	UPDATE SET   ModelName = s.ModelName
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 ModelKey
			,ModelName
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.ModelKey
			,s.ModelName
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
