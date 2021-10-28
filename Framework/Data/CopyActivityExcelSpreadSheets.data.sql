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

 

MERGE dbo.CopyActivityExcelSpreadSheets AS t
  USING (
  VALUES 
(1,'CopyMetaData_AutoFill_SQL','CopyActivityDataSQLScript',1),
(2,'CopyMetaData_AutoFill_SQL','CopyActivityDataADLS',1),
(3,'CopyMetaData_AutoFill_SQL','CopyActivitySink',1),
(4,'CopyMetaData_AutoFill_SQL','CopyActivityParameter',1),
(5,'CopyMetaData_AutoFill_SQL','CopyActivityExecutionPlan',1),
(6,'CopyMetaData_AutoFill_File','CopyActivityDataFileSystem',1),
(7,'CopyMetaData_AutoFill_File','CopyActivityDataADLS',1),
(8,'CopyMetaData_AutoFill_File','CopyActivitySink',1),
(9,'CopyMetaData_AutoFill_File','CopyActivityParameter',1),
(10,'CopyMetaData_AutoFill_File','CopyActivityExecutionPlan',1)

 


    
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
 