/*********************************************************************************************************************
	File: 	Report.data.sql

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

SET IDENTITY_INSERT dbo.Report ON;

MERGE dbo.Report AS r
  USING (
  VALUES 
               (1, 'Sample Report', 'Sample report.', NULL, 'SampleReport.rdl', 1, 1, GetDate(), NULL)
	
              ) AS s
				   (
                    ReportKey
				   ,ReportName
				   ,ReportDescription
				   ,ReportComment
				   ,ReportFileName
				   ,ReportContentKey
				   ,ReportLayoutKey
				   ,CreatedDate
				   ,ModifiedDate
				   )
ON (r.ReportKey = s.ReportKey)
WHEN MATCHED THEN 
       UPDATE SET	ReportName = s.ReportName
				   ,ReportDescription = s.ReportDescription
				   ,ReportComment = s.ReportComment
				   ,ReportFileName = s.ReportFileName
				   ,ReportContentKey = s.ReportContentKey
				   ,ReportLayoutKey = s.ReportLayoutKey
				   ,CreatedDate = s.CreatedDate
				   ,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
			ReportKey
		   ,ReportName
		   ,ReportDescription
		   ,ReportComment
		   ,ReportFileName
		   ,ReportContentKey
		   ,ReportLayoutKey
		   ,CreatedDate
		   ,ModifiedDate
           )    
    VALUES (
			s.ReportKey
		   ,s.ReportName
		   ,s.ReportDescription
		   ,s.ReportComment
		   ,s.ReportFileName
		   ,s.ReportContentKey
		   ,s.ReportLayoutKey
		   ,s.CreatedDate
		   ,s.ModifiedDate
		   );
GO

SET IDENTITY_INSERT dbo.Report OFF;















