/*********************************************************************************************************************
	File: 	PipeLineExecutionGroup.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	08/07/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	08/07/2018	Joseph Barth				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.PipeLineExecutionGroup ON;

MERGE dbo.PipeLineExecutionGroup AS t
  USING (
  VALUES 
		
		 (1,'Retail',1,1,GetDate(),NULL)


		) as s
		(
			 PipeLineExecutionGroupKey
			,PipeLineExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PipeLineExecutionGroupKey = s.PipeLineExecutionGroupKey )
WHEN MATCHED THEN 
	UPDATE SET   PipeLineExecutionGroupName = s.PipeLineExecutionGroupName
				,IsActive = s.IsActive
				,IsEmailEnabled = s.IsEmailEnabled
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PipeLineExecutionGroupKey
			,PipeLineExecutionGroupName
			,IsActive
			,IsEmailEnabled
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PipeLineExecutionGroupKey
			,s.PipeLineExecutionGroupName
			,s.IsActive
			,s.IsEmailEnabled
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.PipeLineExecutionGroup OFF;



