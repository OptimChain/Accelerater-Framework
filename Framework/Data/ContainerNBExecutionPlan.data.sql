

/*********************************************************************************************************************
	File: 	ContainerNBExecutionPlan.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	01/17/2019

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	10/26/2018  Joseph Barth				Created.	
	06/11/2019	Mike Sherrill				Modified for CLA Containers
**********************************************************************************************************************/

-----------------------------------------------------------------------------------
-- Instead of MERGE, using these statements to add ContainerExecutionPlans to the Execution Plan
-- without having to lookup keys for Container and ContainerExecutionGroup.
-----------------------------------------------------------------------------------
DECLARE @DataFactoryName			as varchar(50) = 'am-da-env-adf-01'
DECLARE @ContainerTypeName        as varchar(50) = 'Notebook'

Delete se from dbo.ContainerExecutionPlan se
inner join  [ContainerExecutionGroup] seg on se.ContainerExecutionGroupKey=seg.ContainerExecutionGroupKey
where seg.ContainerExecutionGroupName like '%Stage Process%'
  
INSERT INTO dbo.ContainerExecutionPlan
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism BinServiceStage'  -- CopyActivitySink Execution Group 
	AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
	AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
	AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism DocumentServiceStage'  -- CopyActivitySink Execution Group 
	AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
	AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
	AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism DowntimeServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism GrainInventoryServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism GrainMixServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism GrainReceiptServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism GrainTransferServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism LocationServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism MaterialServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism MillingBinInventoryServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism MillingMasterDataServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism OrderServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism PackFactorServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism PackingServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism PackMasterDataServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism PersonnelAndContactServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism ProductionRunServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism QualityServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism SchedulingServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism ShippingMasterDataServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism ShippingServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Prism TemperTransferServiceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Coms MillsEntOpsStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Coms'
 AND seg.ContainerExecutionGroupName = 'Coms Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
  UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'GP AmusStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'GP AmcanStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'GP CcgStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'GP MolprStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'GP MriceStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 

 UNION ALL

 SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, neg.NotebookExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM NotebookExecutionGroup neg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE neg.NotebookExecutionGroupName = 'Medb MedbStage'  
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Medb'
 AND seg.ContainerExecutionGroupName = 'Medb Stage Process'
 AND sgt.ContainerTypeName = @ContainerTypeName 
 



GO