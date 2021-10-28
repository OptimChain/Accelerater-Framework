/*********************************************************************************************************************
	File: 	DataMapping.data.sql

	Desc: 	Data hydration script.

	Auth: 	Mike Sherrill
	Date: 	7/25/2017

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	07/25/2017	Mike Sherrill				Created.
	11/217/2017	Alan Campbell				Integrated process into the Framework.
**********************************************************************************************************************/

TRUNCATE TABLE dbo.DataMapping

MERGE dbo.DataMapping AS t
  USING (
  VALUES 

	 (1,'Retail','StgCustomer','CustomerID',10,'dbo','DimCustomer','CustomerID','nvarchar(20)',1,GetDate(),NULL)
	,(1,'Retail','StgCustomer','FirstName',10,'dbo','DimCustomer','CustomerFirstName','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','LastName',10,'dbo','DimCustomer','CustomerLastName','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','City',10,'dbo','DimCustomer','CustomerCity','nvarchar(100)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','State',10,'dbo','DimCustomer','CustomerState','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','Birthdate',10,'dbo','DimCustomer','CustomerBirthDate','date',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','MaritalStatus',10,'dbo','DimCustomer','CustomerMaritalStatus','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','Gender',10,'dbo','DimCustomer','CustomerGender','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','YearlyIncome',10,'dbo','DimCustomer','CustomerYearlyIncome','money',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','Education',10,'dbo','DimCustomer','CustomerEducation','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','Occupation',10,'dbo','DimCustomer','CustomerOccupation','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgCustomer','CustomerType',10,'dbo','DimCustomer','CustomerType','nvarchar(50)',0,GetDate(),NULL)

	,(1,'Retail','StgProduct','ProductID',10,'dbo','DimProduct','ProductID','nvarchar(20)',1,GetDate(),NULL)
	,(1,'Retail','StgProduct','ProductLabel',10,'dbo','DimProduct','ProductLabel','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','ProductName',10,'dbo','DimProduct','ProductName','nvarchar(100)',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','ProductSubcategoryName',10,'dbo','DimProduct','ProductSubcategoryName','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','ProductCategoryName',10,'dbo','DimProduct','ProductCategoryName','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','Color',10,'dbo','DimProduct','ProductColor','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','UnitCost',10,'dbo','DimProduct','ProductUnitCost','money',0,GetDate(),NULL)
	,(1,'Retail','StgProduct','UnitPrice',10,'dbo','DimProduct','ProductUnitPrice','money',0,GetDate(),NULL)

	,(1,'Retail','StgStore','StoreID',10,'dbo','DimStore','StoreID','nvarchar(20)',1,GetDate(),NULL)
	,(1,'Retail','StgStore','StoreName',10,'dbo','DimStore','StoreName','nvarchar(100)',0,GetDate(),NULL)
	,(1,'Retail','StgStore','City',10,'dbo','DimStore','StoreCity','nvarchar(100)',0,GetDate(),NULL)
	,(1,'Retail','StgStore','State',10,'dbo','DimStore','StoreState','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgStore','PostalCode',10,'dbo','DimStore','StorePostalCode','nvarchar(10)',0,GetDate(),NULL)
	,(1,'Retail','StgStore','StoreType',10,'dbo','DimStore','StoreType','nvarchar(50)',0,GetDate(),NULL)
	,(1,'Retail','StgStore','SellingAreaSize',10,'dbo','DimStore','StoreSize','int',0,GetDate(),NULL)
	,(1,'Retail','StgStore','StoreStatus',10,'dbo','DimStore','StoreStatus','nvarchar(50)',0,GetDate(),NULL)

	,(1,'Retail','StgSales','SalesOrderNumber',10,'dbo','FactSales','SalesID','nvarchar(20)',1,GetDate(),NULL)
	,(1,'Retail','StgSales','SalesOrderLineNumber',10,'dbo','FactSales','SalesLineID','nvarchar(20)',0,GetDate(),NULL)
	,(1,'Retail','StgSales','OrderDate',10,'dbo','FactSales','OrderDate','date',0,GetDate(),NULL)
	,(1,'Retail','StgSales','StoreID',10,'dbo','FactSales','StoreID','nvarchar(20)',0,GetDate(),NULL)
	,(1,'Retail','StgSales','CustomerID',10,'dbo','FactSales','CustomerID','nvarchar(20)',0,GetDate(),NULL)
	,(1,'Retail','StgSales','ProductID',10,'dbo','FactSales','ProductID','nvarchar(20)',0,GetDate(),NULL)
	,(1,'Retail','StgSales','SalesQuantity',10,'dbo','FactSales','SalesQuantity','decimal(6,2)',0,GetDate(),NULL)
	,(1,'Retail','StgSales','UnitPrice',10,'dbo','FactSales','SalesUnitCost','money',0,GetDate(),NULL)
	,(1,'Retail','StgSales','UnitCost',10,'dbo','FactSales','SalesUnitPrice','money',0,GetDate(),NULL)
	,(1,'Retail','StgSales','SalesAmount',10,'dbo','FactSales','SalesAmount','money',0,GetDate(),NULL)

		) as s
		(
			 SourceKey
			,SourceSchema
			,SourceEntity
			,SourceColumn
			,TargetKey
			,TargetSchema
			,TargetEntity
			,TargetColumn
			,TargetDataType
			,IsBusinessKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.SourceKey = s.SourceKey AND
	 t.SourceSchema = s.SourceSchema AND
	 t.SourceEntity = s.SourceEntity AND 
	 t.SourceColumn = s.SourceColumn)
WHEN MATCHED THEN 
	UPDATE SET   TargetKey = s.TargetKey
				,TargetSchema = s.TargetSchema
				,TargetEntity = s.TargetEntity
				,TargetColumn = s.TargetColumn
				,TargetDataType = s.TargetDataType
				,IsBusinessKey = s.IsBusinessKey
			    ,ModifiedDate = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 SourceKey
			,SourceSchema
			,SourceEntity
			,SourceColumn
			,TargetKey
			,TargetSchema
			,TargetEntity
			,TargetColumn
			,TargetDataType
			,IsBusinessKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.SourceKey
			,s.SourceSchema
			,s.SourceEntity
			,s.SourceColumn
			,s.TargetKey
			,s.TargetSchema
			,s.TargetEntity
			,s.TargetColumn
			,s.TargetDataType
			,s.IsBusinessKey
			,GetDate()
			,NULL
		  );
GO