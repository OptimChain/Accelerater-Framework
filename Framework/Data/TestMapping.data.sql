/*********************************************************************************************************************
	File: 	TestMapping.data.sql

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

-- NOTE:  The start date in this context represents the earliest data loaded into the Data Warehouse
--        and not necessarily the current SystemLoadDate stored in SystemParameter.

DECLARE @StartDate as varchar(20)
SET @StartDate = '1/1/2015'

SET IDENTITY_INSERT dbo.TestMapping ON;

MERGE dbo.TestMapping AS t
  USING (
  VALUES 
		
		 (1,1,1,'COUNT(1)','dbo.Customer','NONE',10,'COUNT(1)','dbo.DimCustomer','NONE',1,GetDate(),NULL)
		,(2,1,1,'COUNT(1)','dbo.Product','NONE',10,'COUNT(1)','dbo.DimProduct','NONE',1,GetDate(),NULL)
		,(3,1,1,'COUNT(1)','dbo.Sales','NONE',10,'COUNT(1)','dbo.FactSales','NONE',1,GetDate(),NULL)
		,(4,1,1,'COUNT(1)','dbo.Store','NONE',10,'COUNT(1)','dbo.DimStore','NONE',1,GetDate(),NULL)		
		,(5,2,10,'COUNT(1)','dbo.DimCustomerView','NONE',11,NULL,'Customer','NONE',1,GetDate(),NULL)
		,(6,2,10,'COUNT(1)','dbo.DimProductView','NONE',11,NULL,'Product','NONE',1,GetDate(),NULL)
		,(7,2,10,'COUNT(1)','dbo.FactSalesView','NONE',11,NULL,'Sales','NONE',1,GetDate(),NULL)
		,(8,2,10,'COUNT(1)','dbo.DimStoreView','NONE',11,NULL,'Store','NONE',1,GetDate(),NULL)
		 		
		) as s
		(
			 TestMappingKey
			,TestGroupKey
			,SourceKey
			,SourceAggregation
			,SourceEntity
			,SourceCondition
			,TargetKey
			,TargetAggregation
			,TargetEntity
			,TargetCondition
			,IsActive
			,CreatedDate
			,ModifiedDate
		)
ON ( t.TestMappingKey = s.TestMappingKey )
WHEN MATCHED THEN 
	UPDATE SET   TestGroupKey = s.TestGroupKey
				,SourceKey = s.SourceKey
				,SourceAggregation = s.SourceAggregation
				,SourceEntity = s.SourceEntity
				,SourceCondition = s.SourceCondition
				,TargetKey = s.TargetKey
				,TargetAggregation = s.TargetAggregation
				,TargetEntity = s.TargetEntity
				,TargetCondition = s.TargetCondition
				,IsActive = s.IsActive
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 TestMappingKey
			,TestGroupKey
			,SourceKey
			,SourceAggregation
			,SourceEntity
			,SourceCondition
			,TargetKey
			,TargetAggregation
			,TargetEntity
			,TargetCondition
			,IsActive
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.TestMappingKey
			,s.TestGroupKey
			,s.SourceKey
			,s.SourceAggregation
			,s.SourceEntity
			,s.SourceCondition
			,s.TargetKey
			,s.TargetAggregation
			,s.TargetEntity
			,s.TargetCondition
			,s.IsActive
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.TestMapping OFF;

