/*********************************************************************************************************************
	File: 	Entity.data.sql

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

MERGE dbo.Entity AS t
  USING (
  VALUES 
		 (10,'Customer','Customer Information.',1,GetDate(),NULL)
		,(20,'Date','Date Information.',1,GetDate(),NULL)
		,(30,'Product','Product Information.',1,GetDate(),NULL)
		,(40,'Sales','Sales Information.',1,GetDate(),NULL)
		,(50,'Store','Store Information.',1,GetDate(),NULL)

		) as s
		(
			 EntityKey
			,EntityName
			,EntityDescription
			,ModelKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.EntityKey = s.EntityKey )
WHEN MATCHED THEN 
	UPDATE SET   EntityName = s.EntityName
				,EntityDescription = s.EntityDescription
				,ModelKey = s.ModelKey
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 EntityKey
			,EntityName
			,EntityDescription
			,ModelKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.EntityKey
			,s.EntityName
			,s.EntityDescription
			,s.ModelKey
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
