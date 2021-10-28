/*********************************************************************************************************************
	File: 	TestGroup.data.sql

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

SET IDENTITY_INSERT dbo.TestGroup ON;

MERGE dbo.TestGroup AS t
  USING (
  VALUES 
		
		 (1,'Retail vs Data Warehouse',1,GetDate(),NULL)
		,(2,'Data Warehouse vs Tabular',1,GetDate(),NULL)
		 		
		) as s
		(
			 TestGroupKey
			,TestGroupName
			,IsActive
			,CreatedDate
			,ModifiedDate
		)
ON ( t.TestGroupKey = s.TestGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   TestGroupName = s.TestGroupName
				,IsActive = s.IsActive
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 TestGroupKey
			,TestGroupName
			,IsActive
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.TestGroupKey
			,s.TestGroupName
			,s.IsActive
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.TestGroup OFF;