


/*********************************************************************************************************************
	File: 	CopyActivityExecutionPlan.data.sql

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
	12/16/2019	Mike Sherrill				Modified for Sundt Containers
**********************************************************************************************************************/

-----------------------------------------------------------------------------------
-- Instead of MERGE, using these statements to add ContainerExecutionPlans to the Execution Plan
-- without having to lookup keys for Container and ContainerExecutionGroup.
-----------------------------------------------------------------------------------

Delete se from dbo.CopyActivityExecutionPlan se

INSERT INTO dbo.CopyActivityExecutionPlan (CopyActivitySinkKey, ContainerKey, CopyActivityExecutionGroupKey, CopyActivityOrder, IsActive, IsEmailEnabled, IsRestart, CreatedDate, ModifiedDate )
-- AX
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTJOURNALTABLE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTJOURNALTRANS' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_InventTransferTable' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_InventTransferLine' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_BTLTransferReleaseHistoryLog' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_PurchTable' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_Purchline' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTRANS' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTRANSPOSTING' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_REQTRANS' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_RETAILCHANNELTABLE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTABLE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_SMMCAMPAIGNTABLE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_RETAILCAMPAIGNDISCOUNT' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_ECORESCATEGORY' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_ECORESPRODUCT' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_ECORESPRODUCTCATEGORY' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_OMHIERARCHYRELATIONSHIP' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_DIRORGANIZATIONNAME' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_REQTRANSFIRMLOG' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_UNITOFMEASURE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_UNITOFMEASURECONVERSION' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTABLEMODULE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTRANSFERJOUR' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,0,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_INVENTTRANSFERJOURLINE' UNION ALL
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'AX Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'AX_DIRPARTYTABLE' UNION ALL


--K3S
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'K3SNoHeader Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'K3S_AssistedOrders' UNION ALL

--McKesson
SELECT sk.CopyActivitySinkKey, pr.ContainerKey, peg.CopyActivityExecutionGroupKey,10,1,0,1,GetDate(),NULL FROM CopyActivityExecutionGroup peg, Container pr, CopyActivitySink sk WHERE peg.CopyActivityExecutionGroupName = 'McKesson Copy Process' and pr.ContainerName = 'brtl' and sk.CopyActivitySinkName = 'McKesson_FILL_FACT' 

GO
