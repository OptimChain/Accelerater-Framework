/*********************************************************************************************************************
	File: 	ReportContent.data.sql

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

SET IDENTITY_INSERT dbo.ReportContent ON;

MERGE dbo.ReportContent AS c
  USING (
  VALUES 
              (1, 'Standard', 'Standard Content', GetDate(), NULL)		              
              ) AS s
				   (
                    ReportContentKey
				   ,ReportContentName
				   ,ReportContentDescription
				   ,CreatedDate
				   ,ModifiedDate
				   )
ON (c.ReportContentKey = s.ReportContentKey)
WHEN MATCHED THEN 
       UPDATE SET	ReportContentName = s.ReportContentName
				   ,ReportContentDescription = s.ReportContentDescription
				   ,CreatedDate = s.CreatedDate
				   ,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
			ReportContentKey
		   ,ReportContentName
		   ,ReportContentDescription
		   ,CreatedDate
		   ,ModifiedDate
           )    
    VALUES (
			s.ReportContentKey
		   ,s.ReportContentName
		   ,s.ReportContentDescription
		   ,s.CreatedDate
		   ,s.ModifiedDate
		   );
GO

SET IDENTITY_INSERT dbo.ReportContent OFF;
