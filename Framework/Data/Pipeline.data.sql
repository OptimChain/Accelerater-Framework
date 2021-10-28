  /*********************************************************************************************************************
	File: 	PipeLine.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	06/07/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	04/30/2018	Joseph Barth				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.PipeLine ON;

MERGE dbo.PipeLine AS t
  USING (
  VALUES 
	     (1,'RetailMaster',NULL,NULL,GetDate(),NULL)
		,(2,'RetailProcessCopy',NULL,NULL,GetDate(),NULL)
		,(3,'RetailProcessStage',NULL,NULL,GetDate(),NULL)
		,(4,'RetailProcedureLoop',NULL,NULL,GetDate(),NULL)
		,(5,'RetailReset',NULL,NULL,GetDate(),NULL)
		,(6,'RetailCopyLoop',NULL,NULL,GetDate(),NULL)
		,(7,'RetailStageLoop',NULL,NULL,GetDate(),NULL)
		,(8,'RetailProcedure',NULL,NULL,GetDate(),NULL)
		,(9,'RetailCopy',NULL,NULL,GetDate(),NULL)
		,(10,'RetailStage',NULL,NULL,GetDate(),NULL)
		 
		 
		 ) as s
		(
			 PipeLineKey
			,PipeLineName
			,PipeLineCreationDate
			,PipeLineCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PipeLineKey = s.PipeLineKey )
WHEN MATCHED THEN 
	UPDATE SET   PipeLineName = s.PipeLineName
				,PipeLineCreationDate = s.PipeLineCreationDate
				,PipeLineCreatedBy = s.PipeLineCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PipeLineKey
			,PipeLineName
			,PipeLineCreationDate
			,PipeLineCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PipeLineKey
			,s.PipeLineName
			,s.PipeLineCreationDate
			,s.PipeLineCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.PipeLine OFF;
