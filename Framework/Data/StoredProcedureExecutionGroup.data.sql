

/*********************************************************************************************************************
       File: StoredProcedureExecutionGroup.data.sql

       Desc: Data hydration script.

       Auth: Joseph Barth
       Date: 08/16/2018

       NOTE:  
         

==================================================================================================================
    Change History
==================================================================================================================
       Date         Author                                  Description
       ----------- ---------------------------  --------------------------------------------------------------------------
       08/16/2018    Joseph Barth                      Created.      
       09/18/2018    Alan Campbell                     Added some additional groups to align better with other Framework examples.
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.StoredProcedureExecutionGroup ON;

MERGE dbo.StoredProcedureExecutionGroup AS t
  USING (
  VALUES 
             
                 (1,'Prism_DimA',1,1,GetDate(),NULL)
				,(2,'Prism_DimB',1,1,GetDate(),NULL)
				,(3,'Prism_DimC',1,1,GetDate(),NULL)
				,(4,'Prism_FactA',1,1,GetDate(),NULL)
				,(5,'Prism_FactB',1,1,GetDate(),NULL)
				,(6,'Prism_FactC',1,1,GetDate(),NULL)
				,(7,'Prism_FactD',1,1,GetDate(),NULL)
				,(8,'Coms_DimA',1,1,GetDate(),NULL)
				,(9,'Coms_DimB',1,1,GetDate(),NULL)
				,(10,'Coms_DimC',1,1,GetDate(),NULL)
				,(11,'Coms_FactA',1,1,GetDate(),NULL)
				,(12,'Coms_FactB',1,1,GetDate(),NULL)
				,(13,'Coms_FactC',1,1,GetDate(),NULL)
				,(14,'Coms_FactD',1,1,GetDate(),NULL)
				,(15,'GP_DimA',1,1,GetDate(),NULL)
				,(16,'GP_DimB',1,1,GetDate(),NULL)
				,(17,'GP_DimC',1,1,GetDate(),NULL)
				,(18,'GP_FactA',1,1,GetDate(),NULL)
				,(19,'GP_FactB',1,1,GetDate(),NULL)
				,(20,'GP_FactC',1,1,GetDate(),NULL)
				,(21,'GP_FactD',1,1,GetDate(),NULL)
				,(22,'Workday',1,1,GetDate(),NULL)	



             ) as s
             (
                    StoredProcedureExecutionGroupKey
                    ,StoredProcedureExecutionGroupName
                    ,IsActive
                    ,IsEmailEnabled
                    ,CreatedDate
                    ,ModifiedDate
             )
ON ( t.StoredProcedureExecutionGroupKey = s.StoredProcedureExecutionGroupKey )
WHEN MATCHED THEN 
       UPDATE SET   StoredProcedureExecutionGroupName = s.StoredProcedureExecutionGroupName
                           ,IsActive = s.IsActive
                           ,IsEmailEnabled = s.IsEmailEnabled
                           ,CreatedDate = s.CreatedDate
                           ,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
                    StoredProcedureExecutionGroupKey
                    ,StoredProcedureExecutionGroupName
                    ,IsActive
                    ,IsEmailEnabled
                    ,CreatedDate
                    ,ModifiedDate
               )    
    VALUES(
                    s.StoredProcedureExecutionGroupKey
                    ,s.StoredProcedureExecutionGroupName
                    ,s.IsActive
                    ,s.IsEmailEnabled
                    ,s.CreatedDate
                    ,s.ModifiedDate
               );
GO

SET IDENTITY_INSERT dbo.StoredProcedureExecutionGroup OFF;
