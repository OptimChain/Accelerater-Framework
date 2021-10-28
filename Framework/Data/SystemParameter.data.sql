/*********************************************************************************************************************
	File: 	SystemParameter.data.sql

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

SET IDENTITY_INSERT dbo.SystemParameter ON;

MERGE dbo.SystemParameter AS t
  USING (
  VALUES 
		 (1,'LoadDate','1/1/2014',1,GetDate(),NULL),
         (2,'IncrementalDays','-3',1, GetDate(),NULL)
		
		) as s
		(
			 SystemParameterKey
			,SystemParameterName
			,SystemParameterValue
			,SystemKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.SystemParameterKey = s.SystemParameterKey )
WHEN MATCHED THEN 
	UPDATE SET   SystemParameterName = s.SystemParameterName
			    ,SystemParameterValue = s.SystemParameterValue
				,SystemKey = s.SystemKey
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 SystemParameterKey
			,SystemParameterName
			,SystemParameterValue
			,SystemKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.SystemParameterKey
			,s.SystemParameterName
			,s.SystemParameterValue
			,s.SystemKey
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.SystemParameter OFF;