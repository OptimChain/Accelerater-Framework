/*********************************************************************************************************************
	File: 	ContainerGroup.data.sql

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

SET IDENTITY_INSERT dbo.ContainerGroup ON;

MERGE dbo.ContainerGroup AS t
  USING (
  VALUES 
  		 (1,'Client',NULL,NULL,GetDate(),NULL),
		 (2,'Firm',NULL,NULL,GetDate(),NULL),
		 (3,'Benchmark',NULL,NULL,GetDate(),NULL),
		 (4,'AdHoc',NULL,NULL,GetDate(),NULL)
		 ) as s
		(
			 ContainerGroupKey
			,ContainerGroupName
			,ContainerGroupDate
			,ContainerGroupCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.ContainerGroupKey = s.ContainerGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   ContainerGroupName = s.ContainerGroupName
				,ContainerGroupDate = s.ContainerGroupDate
				,ContainerGroupCreatedBy = s.ContainerGroupCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 ContainerGroupKey
			,ContainerGroupName
			,ContainerGroupDate
			,ContainerGroupCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.ContainerGroupKey
			,s.ContainerGroupName
			,s.ContainerGroupDate
			,s.ContainerGroupCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.ContainerGroup OFF;
