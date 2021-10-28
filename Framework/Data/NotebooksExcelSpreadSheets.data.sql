/*********************************************************************************************************************
    File:     SpreadSheet.data.sql

 

    Desc:     Data hydration script.

 

    Auth:     Alan Campbell
    Date:     2/14/2018

 

    NOTE:    
         

 

    ==================================================================================================================
    Change History
    ==================================================================================================================
    Date        Author                        Description
    ----------- ---------------------------    --------------------------------------------------------------------------
    6/11/2020    Chaitanya Badam                Created.            
**********************************************************************************************************************/

 

MERGE dbo.NotebooksExcelSpreadSheets AS t
  USING (
  VALUES 
(1,'NotebookParameters_AutoFill_Framework','Convert',1),
(2,'NotebookParameters_AutoFill_Framework','Ingest',1),
(3,'NotebookParameters_AutoFill_Framework','Enrich',1),
(4,'NotebookParameters_AutoFill_Framework','Sanction',1)
        ) as s
        (
             ID
            ,ExcelName
            ,ExcelSpreadSheetName
            ,IsActive
        )
ON ( t.ID = s.ID )
WHEN MATCHED THEN 
    UPDATE SET   ExcelName = s.ExcelName
                ,ExcelSpreadSheetName = s.ExcelSpreadSheetName
                ,IsActive = s.IsActive
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
             ID
            ,ExcelName
            ,ExcelSpreadSheetName
            ,IsActive
          )    
    VALUES(
             s.ID
            ,s.ExcelName
            ,s.ExcelSpreadSheetName
            ,s.IsActive
          );
GO
 