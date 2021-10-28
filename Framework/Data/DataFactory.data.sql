/*********************************************************************************************************************
	File: 	DataFactory.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	10/15/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	10/15/2018	Joseph Barth				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.DataFactory ON;

MERGE dbo.DataFactory AS t
  USING (
  VALUES 

		 (1,'neu-pe-fwk-df-01',NULL,NULL,GetDate(),NULL)
		 ) as s
		(
			 DataFactoryKey
			,DataFactoryName
			,DataFactoryDate
			,DataFactoryCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.DataFactoryKey = s.DataFactoryKey )
WHEN MATCHED THEN 
	UPDATE SET   DataFactoryName = s.DataFactoryName
				,DataFactoryDate = s.DataFactoryDate
				,DataFactoryCreatedBy = s.DataFactoryCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 DataFactoryKey
			,DataFactoryName
			,DataFactoryDate
			,DataFactoryCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.DataFactoryKey
			,s.DataFactoryName
			,s.DataFactoryDate
			,s.DataFactoryCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.DataFactory OFF;
