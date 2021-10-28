/*********************************************************************************************************************
	File: 	DataSource.data.sql

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

SET IDENTITY_INSERT dbo.DataSource ON;

MERGE dbo.DataSource AS t
  USING (
  VALUES 
		 (1,'Retail',1,GetDate(),NULL)
		,(10,'DataWarehouse',1,GetDate(),NULL)
		,(11,'Tabular',3,GetDate(),NULL)		
		) as s
		(
			 DataSourceKey
			,DataSourceName
			,DataSourceTypeKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.DataSourceKey = s.DataSourceKey )
WHEN MATCHED THEN 
	UPDATE SET   DataSourceName = s.DataSourceName
			    ,DataSourceTypeKey = s.DataSourceTypeKey
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 DataSourceKey
			,DataSourceName
			,DataSourceTypeKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.DataSourceKey
			,s.DataSourceName
			,s.DataSourceTypeKey
			,s.CreatedDate
			,s.ModifiedDate
		  );

GO

SET IDENTITY_INSERT dbo.DataSource OFF;
