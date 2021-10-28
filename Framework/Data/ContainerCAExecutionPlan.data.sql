


/*********************************************************************************************************************
	File: 	ContainerCAExecutionPlan.data.sql

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
DECLARE @ContainerTypeName        as varchar(50) = 'CopyActivity'

Delete se from dbo.ContainerExecutionPlan se
inner join  [ContainerExecutionGroup] seg on se.ContainerExecutionGroupKey=seg.ContainerExecutionGroupKey
where seg.ContainerExecutionGroupName like '%Copy Process%'
  
INSERT INTO dbo.ContainerExecutionPlan
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Binservice Process'  -- CopyActivitySink Execution Group 
	AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
	AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
	AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Document Service Process'  -- CopyActivitySink Execution Group 
	AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
	AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
	AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Downtime Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Grain Inventory Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Grain Mix Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Grain Receipt Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Grain Transfer Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Location Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Material Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Milling Bin Inventory Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Milling Master Data Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Order Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Pack Factor Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Packing Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Pack Master Data Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Personnel And Contact Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Production Run Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Quality Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Scheduling Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Shipping Master Data Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Shipping Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Prism Temper Transfer Service Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Prism'
 AND seg.ContainerExecutionGroupName = 'Prism Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
 UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Coms MillsEntOps Process' 
 AND d.DataFactoryName = @DataFactoryName 
 and pr.ContainerName = 'Coms'
 AND seg.ContainerExecutionGroupName = 'Coms Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName

UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'GP Amus Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'GP Amcan Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'GP Ccg Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'GP Molpr Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
UNION ALL
SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'GP Mrice Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'GP'
 AND seg.ContainerExecutionGroupName = 'GP Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName
 UNION ALL
 SELECT pr.ContainerKey,d.DataFactoryKey,seg.ContainerExecutionGroupKey, peg.CopyActivityExecutionGroupKey,sgt.[ContainerTypeKey],10,1,0,1,GetDate(),NULL
FROM CopyActivityExecutionGroup peg,DataFactory d, Container pr,ContainerType sgt,[ContainerExecutionGroup] seg
WHERE peg.CopyActivityExecutionGroupName = 'Medb Medb Process' 
 AND d.DataFactoryName = @DataFactoryName and pr.ContainerName = 'Medb'
 AND seg.ContainerExecutionGroupName = 'Medb Copy Process'
 AND sgt.ContainerTypeName = @ContainerTypeName



GO
