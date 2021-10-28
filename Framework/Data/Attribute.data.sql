/*********************************************************************************************************************
	File: 	Attribute.data.sql

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

MERGE dbo.Attribute AS t
  USING (
  VALUES 
		 (10,'Avg Yearly Income','Average yearly income.',10,GetDate(),NULL)
		,(20,'Customer Birthdate','Customer birth date.',10,GetDate(),NULL)
		,(30,'Customer City','The city where the Customer is located.',10,GetDate(),NULL)
		,(40,'Customer Education','Customer education level.',10,GetDate(),NULL)
		,(50,'Customer First Name','The first name for the Customer.',10,GetDate(),NULL)
		,(60,'Customer Gender','The gender for the Customer.',10,GetDate(),NULL)
		,(70,'Customer ID','A unique identifier for the Customer.',10,GetDate(),NULL)
		,(80,'Customer Last Name','The last name for the Customer.',10,GetDate(),NULL)
		,(90,'Customer Marital Status','Indicates the marital status for a Customer.',10,GetDate(),NULL)
		,(100,'Customer Occupation','Indicates the occupation for a Customer.',10,GetDate(),NULL)
		,(110,'Customer State','The state where the Customer is located.',10,GetDate(),NULL)
		,(120,'Customer Type','The type of Customer.',10,GetDate(),NULL)
		,(130,'Customers','The count of Customers within the current context.',10,GetDate(),NULL)
		,(140,'Day','Indicates the day of the year.',20,GetDate(),NULL)
		,(150,'Month','Indicates the month of the year.',20,GetDate(),NULL)
		,(160,'Week','Indicates the week of the year.',20,GetDate(),NULL)
		,(170,'Year','Indicates the year.',20,GetDate(),NULL)
		,(180,'Product Category','The product category for this product.',30,GetDate(),NULL)
		,(190,'Product Color','The color of the product.',30,GetDate(),NULL)
		,(200,'Product ID','A unique identifier for the product.',30,GetDate(),NULL)
		,(210,'Product Label','The label found on product packaging.',30,GetDate(),NULL)
		,(220,'Product Name','The name given to the product.',30,GetDate(),NULL)
		,(230,'Product SubCategory','The subcategory this product belongs to.',30,GetDate(),NULL)
		,(240,'Products','The count of products within the current context.',30,GetDate(),NULL)
		,(250,'Cost','The cost of the product sold.',40,GetDate(),NULL)
		,(260,'Margin','The margin earned on the product sold.',40,GetDate(),NULL)
		,(270,'Margin %','The % margin earned on the product sold.',40,GetDate(),NULL)
		,(280,'Sales','The sales earned on the product sold.',40,GetDate(),NULL)
		,(290,'Sales ID','A unique identifier for the sale.',40,GetDate(),NULL)
		,(300,'Sales Line ID','Indicates the line number on the order.',40,GetDate(),NULL)
		,(310,'Volume','Indicates the number of units sold.',40,GetDate(),NULL)
		,(320,'Avg Store sq ft','The average square feet fro the store.',50,GetDate(),NULL)
		,(330,'Store City','The city where the Store is located.',50,GetDate(),NULL)
		,(340,'Store ID','A unique identiifer for the Store.',50,GetDate(),NULL)
		,(350,'Store Name','The name given to the Store.',50,GetDate(),NULL)
		,(360,'Store Postal Code','The postal code where the Store is located.',50,GetDate(),NULL)
		,(370,'Store State','The state where the Store is located.',50,GetDate(),NULL)
		,(380,'Store Status','The current status for the Store.',50,GetDate(),NULL)
		,(390,'Store Type','Indicates the type of Store.',50,GetDate(),NULL)
		,(400,'Stores','The count of Stores within the current context.',50,GetDate(),NULL)
		,(410,'Total Store sq ft','The total square feet for the Store.',50,GetDate(),NULL)

	
		) as s
		(
			 AttributeKey
			,AttributeName
			,AttributeDescription
			,EntityKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.AttributeKey = s.AttributeKey )
WHEN MATCHED THEN 
	UPDATE SET   AttributeName = s.AttributeName
				,AttributeDescription = s.AttributeDescription
				,EntityKey = s.EntityKey
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 AttributeKey
			,AttributeName
			,AttributeDescription
			,EntityKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.AttributeKey
			,s.AttributeName
			,s.AttributeDescription
			,s.EntityKey
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
