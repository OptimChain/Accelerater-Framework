/*********************************************************************************************************************
	File: 	DataSourceType.data.sql

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

SET IDENTITY_INSERT dbo.DataSourceType ON;

MERGE dbo.DataSourceType AS t
  USING (
  VALUES 
		 (1,'SQL',GetDate(),NULL)
		,(2,'OLAP',GetDate(),NULL)
		,(3,'BISM',GetDate(),NULL)
		,(4,'ADLS',GetDate(),NULL)
		,(5,'FILE',GetDate(),NULL)
		
		) as s
		(
			 DataSourceTypeKey
			,DataSourceTypeName
			,CreatedDate
			,ModifiedDate
		)
ON ( t.DataSourceTypeKey = s.DataSourceTypeKey )
WHEN MATCHED THEN 
	UPDATE SET   DataSourceTypeName = s.DataSourceTypeName
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 DataSourceTypeKey
			,DataSourceTypeName
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.DataSourceTypeKey
			,s.DataSourceTypeName
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.DataSourceType OFF;
