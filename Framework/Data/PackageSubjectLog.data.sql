/*********************************************************************************************************************
	File: 	PackageSubjectLog.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	03/16/2016

	NOTE:	This is temporary hydration to show how the Package Subject report works.  The real data will come from
	        the Master package.
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	03/16/2016	Alan Campbell				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.PackageSubjectLog ON;

MERGE dbo.PackageSubjectLog AS t
  USING (
  VALUES 
		
		 (1,'Business Subject 1','<Description>','Successful','7/29/2015','1/1/2015','7/28/2015',1,GetDate(),NULL)
		,(2,'Business Subject 2','<Description>','Successful','7/29/2015','1/1/2015','7/28/2015',1,GetDate(),NULL)
		 		
		) as s
		(
			 PackageSubjectKey
			,PackageSubjectName
			,PackageSubjectDescription
			,PackageSubjectStatus
			,PackageSubjectEffectiveDate
			,PackageSubjectStartDate
			,PackageSubjectEndDate
			,PackageExecutionLogKey
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PackageSubjectKey = s.PackageSubjectKey )
WHEN MATCHED THEN 
	UPDATE SET   PackageSubjectName = s.PackageSubjectName
	            ,PackageSubjectDescription = s.PackageSubjectDescription
				,PackageSubjectStatus = s.PackageSubjectStatus
				,PackageSubjectEffectiveDate = s.PackageSubjectEffectiveDate
				,PackageSubjectStartDate = s.PackageSubjectStartDate
				,PackageSubjectEndDate = s.PackageSubjectEndDate
				,PackageExecutionLogKey = s.PackageExecutionLogKey
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PackageSubjectKey
			,PackageSubjectName
			,PackageSubjectDescription
			,PackageSubjectStatus
			,PackageSubjectEffectiveDate
			,PackageSubjectStartDate
			,PackageSubjectEndDate
			,PackageExecutionLogKey
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PackageSubjectKey
			,s.PackageSubjectName
			,s.PackageSubjectDescription
			,s.PackageSubjectStatus
			,s.PackageSubjectEffectiveDate
			,s.PackageSubjectStartDate
			,s.PackageSubjectEndDate
			,s.PackageExecutionLogKey
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.PackageSubjectLog OFF;