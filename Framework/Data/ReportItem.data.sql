/*********************************************************************************************************************
	File: 	ReportItem.data.sql

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

SET IDENTITY_INSERT dbo.ReportItem ON;

MERGE dbo.ReportItem AS i
  USING (
  VALUES 
              (1, 'CopyrightNotice', CONCAT('© Copyright ', YEAR(GetDate()), ' Neudesic, LLC.'), 1, GetDate(), NULL)		              
			 ,(2, 'Logo','<url for logo>', 1, GetDate(), NULL)
			 ,(3, 'Disclaimer', '<Disclaimer Statement>', 1, GetDate(), NULL)
              ) AS s
				   (
                    ReportItemKey
				   ,ReportItemName
				   ,ReportItemValue
				   ,ReportContentKey
				   ,CreatedDate
				   ,ModifiedDate
				   )
ON (i.ReportItemKey = s.ReportItemKey)
WHEN MATCHED THEN 
       UPDATE SET	ReportItemName = s.ReportItemName
				   ,ReportItemValue = s.ReportItemValue
				   ,ReportContentKey = s.ReportContentKey
				   ,CreatedDate = s.CreatedDate
				   ,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
			ReportItemKey
		   ,ReportItemName
		   ,ReportItemValue
		   ,ReportContentKey
		   ,CreatedDate
		   ,ModifiedDate
           )    
    VALUES (
			s.ReportItemKey
		   ,s.ReportItemName
		   ,s.ReportItemValue
		   ,s.ReportContentKey
		   ,s.CreatedDate
		   ,s.ModifiedDate
		   );
GO

SET IDENTITY_INSERT dbo.ReportItem OFF;



