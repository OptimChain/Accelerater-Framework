/*********************************************************************************************************************
	File: 	Package.data.sql

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

SET IDENTITY_INSERT dbo.Package ON;

MERGE dbo.Package AS t
  USING (
  VALUES 

		 (1,NULL,'Master',NULL,NULL,GetDate(),NULL)
		,(2,NULL,'RetailStgCustomer',NULL,NULL,GetDate(),NULL)
		,(3,NULL,'RetailStgProduct',NULL,NULL,GetDate(),NULL)
		,(4,NULL,'RetailStgSales',NULL,NULL,GetDate(),NULL)
		,(5,NULL,'RetailStgStore',NULL,NULL,GetDate(),NULL)	
		,(6,NULL,'RetailDimCustomer',NULL,NULL,GetDate(),NULL)
		,(7,NULL,'RetailDimProduct',NULL,NULL,GetDate(),NULL)
		,(8,NULL,'RetailDimStore',NULL,NULL,GetDate(),NULL)
		,(9,NULL,'RetailFactSales',NULL,NULL,GetDate(),NULL)
		,(10,NULL,'FrameworkProcess',NULL,NULL,GetDate(),NULL)
		,(11,NULL,'RetailProcess',NULL,NULL,GetDate(),NULL)
		,(12,NULL,'TestStatic',NULL,NULL,GetDate(),NULL)
		,(13,NULL,'UtlMaintenance',NULL,NULL,GetDate(),NULL)
		,(14,NULL,'SubscriptionDaily',NULL,NULL,GetDate(),NULL)
		,(15,NULL,'RetailStoreCopy',NULL,NULL,GetDate(),NULL)
		
		) as s
		(
			 PackageKey
			,PackageGUID
			,PackageName
			,PackageCreationDate
			,PackageCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PackageKey = s.PackageKey )
WHEN MATCHED THEN 
	UPDATE SET   PackageGUID = s.PackageGUID
				,PackageName = s.PackageName
				,PackageCreationDate = s.PackageCreationDate
				,PackageCreatedBy = s.PackageCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PackageKey
			,PackageGUID
			,PackageName
			,PackageCreationDate
			,PackageCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PackageKey
			,s.PackageGUID
			,s.PackageName
			,s.PackageCreationDate
			,s.PackageCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.Package OFF;
