/*********************************************************************************************************************
	File: 	ReportElement.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	03/16/2016

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	03/16/2016	Adam Sequoyah				Created.			
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.ReportElement ON;

MERGE dbo.ReportElement AS e
  USING (
  VALUES 
              (1, 'TitleFontSize', '20pt', 1, GetDate())
			 ,(2, 'TitleFontFamily', 'Tahoma', 1, GetDate())
			 ,(3, 'TitleFontColor', '#034C1F', 1, GetDate())	-- ExamOne's third green swatch
			 ,(4, 'TableHeaderBackgroundColor', '#007934', 1, GetDate())	-- ExamOne's second green swatch
			 ,(5, 'TableHeaderFontColor', 'White', 1, GetDate())
			 ,(6, 'TableHeaderFontFamily', 'Tahoma', 1, GetDate())
			 ,(7, 'TableHeaderFontSize', '10pt', 1, GetDate())
              ) AS s
				   (
                    ReportElementKey
				   ,ReportElementName
				   ,ReportElementValue
				   ,ReportLayoutKey
				   ,CreatedDate
				   )
ON (e.ReportElementKey = s.ReportElementKey)
WHEN MATCHED THEN 
       UPDATE SET	ReportElementName = s.ReportElementName
				   ,ReportElementValue = s.ReportElementValue
				   ,ReportLayoutKey = s.ReportLayoutKey
				   ,CreatedDate = s.CreatedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
			ReportElementKey
		   ,ReportElementName
		   ,ReportElementValue
		   ,ReportLayoutKey
		   ,CreatedDate
           )    
    VALUES (
			s.ReportElementKey
		   ,s.ReportElementName
		   ,s.ReportElementValue
		   ,s.ReportLayoutKey
		   ,s.CreatedDate
		   );
GO

SET IDENTITY_INSERT dbo.ReportElement OFF;


