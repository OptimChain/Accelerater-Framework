/*********************************************************************************************************************
	File: 	ReportLayout.data.sql

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

SET IDENTITY_INSERT dbo.ReportLayout ON;

MERGE dbo.ReportLayout AS L
  USING (
  VALUES 
              (1, 'StandardGreen', 'Standard Green Layout', GetDate(), NULL)		              
              ) AS s
				   (
                    ReportLayoutKey
				   ,ReportLayoutName
				   ,ReportLayoutDescription
				   ,CreatedDate
				   ,ModifiedDate
				   )
ON (L.ReportLayoutKey = s.ReportLayoutKey)
WHEN MATCHED THEN 
       UPDATE SET	ReportLayoutName = s.ReportLayoutName
				   ,ReportLayoutDescription = s.ReportLayoutDescription
				   ,CreatedDate = s.CreatedDate
				   ,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
			ReportLayoutKey
		   ,ReportLayoutName
		   ,ReportLayoutDescription
		   ,CreatedDate
		   ,ModifiedDate
           )    
    VALUES (
			s.ReportLayoutKey
		   ,s.ReportLayoutName
		   ,s.ReportLayoutDescription
		   ,s.CreatedDate
		   ,s.ModifiedDate
		   );
GO

SET IDENTITY_INSERT dbo.ReportLayout OFF;
