/*********************************************************************************************************************
	File: 	EntityRelationship.data.sql

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

MERGE dbo.EntityRelationship AS t
  USING (
  VALUES 
		 (1,10,40,1,GetDate(),NULL)
		,(1,20,40,1,GetDate(),NULL)
		,(1,30,40,1,GetDate(),NULL)
		,(1,50,40,1,GetDate(),NULL)

		) as s
		(
			 ModelKey
			,DimensionEntityKey
			,FactEntityKey
			,IsRelated
			,CreatedDate
			,ModifiedDate
		)
ON ( t.ModelKey = s.ModelKey AND t.DimensionEntityKey = s.DimensionEntityKey AND t.FactEntityKey = s.FactEntityKey )
WHEN MATCHED THEN 
	UPDATE SET   IsRelated = s.IsRelated
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 ModelKey
			,DimensionEntityKey
			,FactEntityKey
			,IsRelated
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.ModelKey
			,s.DimensionEntityKey
			,s.FactEntityKey
			,s.IsRelated
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
