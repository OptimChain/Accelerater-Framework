/*********************************************************************************************************************
	File: 	PackageLoadPlan.data.sql

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
DECLARE @StartDate as datetime
SET @StartDate = '1/1/2014 12:00 AM'

MERGE dbo.PackageLoadPlan AS t
  USING (
  VALUES 

		 (2,@StartDate,0,NULL,GetDate(),NULL)
		,(3,@StartDate,0,NULL,GetDate(),NULL)
		,(4,@StartDate,0,NULL,GetDate(),NULL)
		,(5,@StartDate,0,NULL,GetDate(),NULL)
		
		) as s
		(
			 PackageKey
			,PackageLoadPlan
			,IsIncrementalLoad
			,PackageLastLoad
			,CreatedDate
			,ModifiedDate
		)
ON ( t.PackageKey = s.PackageKey )
WHEN MATCHED THEN 
	UPDATE SET   IsIncrementalLoad = s.IsIncrementalLoad 
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 PackageKey
			,PackageLoadPlan
			,IsIncrementalLoad
			,PackageLastLoad
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.PackageKey
			,s.PackageLoadPlan
			,s.IsIncrementalLoad
			,s.PackageLastLoad
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
