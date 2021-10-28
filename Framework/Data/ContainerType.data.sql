/*********************************************************************************************************************
	File: 	ContainerType.data.sql

	Desc: 	Data hydration script.

	Auth: 	Mike Sherrill
	Date: 	06/11/2019

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	06/11/2019	Mike Sherrill				Created.
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.ContainerType ON;

MERGE dbo.ContainerType AS t
  USING (
  VALUES 
  		 (1,'brtl',NULL,NULL,GetDate(),NULL)
		 ) as s
		(
			 ContainerTypeKey
			,ContainerTypeName
			,ContainerTypeDate
			,ContainerTypeCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.ContainerTypeKey = s.ContainerTypeKey )
WHEN MATCHED THEN 
	UPDATE SET   ContainerTypeName = s.ContainerTypeName
				,ContainerTypeDate = s.ContainerTypeDate
				,ContainerTypeCreatedBy = s.ContainerTypeCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 ContainerTypeKey
			,ContainerTypeName
			,ContainerTypeDate
			,ContainerTypeCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.ContainerTypeKey
			,s.ContainerTypeName
			,s.ContainerTypeDate
			,s.ContainerTypeCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.ContainerType OFF;
