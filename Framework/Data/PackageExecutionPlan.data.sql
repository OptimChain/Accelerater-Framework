/*********************************************************************************************************************
	File: 	PackageExecutionPlan.data.sql

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

-----------------------------------------------------------------------------------
-- Instead of MERGE, using these statements to add packages to the Execution Plan
-- without having to lookup keys for Package and PackageExecutionGroup.
-----------------------------------------------------------------------------------

DELETE FROM dbo.PackageExecutionPlan
GO

-- Master Packages

INSERT INTO dbo.PackageExecutionPlan
SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Master'  -- Package Execution Group
WHERE p.PackageName = 'Master'  -- Package Name
UNION ALL

-- Staging Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Staging A'  -- Package Execution Group
WHERE p.PackageName = 'RetailStgCustomer'  -- Package Name
UNION ALL
SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Staging B'  -- Package Execution Group
WHERE p.PackageName = 'RetailStgProduct'  -- Package Name
UNION ALL
SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Staging C'  -- Package Execution Group
WHERE p.PackageName = 'RetailStgSales'  -- Package Name
UNION ALL
SELECT p.PackageKey, peg.PackageExecutionGroupKey,20,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Staging C'  -- Package Execution Group
WHERE p.PackageName = 'RetailStgStore'  -- Package Name
UNION ALL

-- Dimension Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Dimension A'  -- Package Execution Group
WHERE p.PackageName = 'RetailDimCustomer'  -- Package Name
UNION ALL
SELECT p.PackageKey, peg.PackageExecutionGroupKey,20,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Dimension A'  -- Package Execution Group
WHERE p.PackageName = 'RetailDimProduct'  -- Package Name
UNION ALL
SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Dimension B'  -- Package Execution Group
WHERE p.PackageName = 'RetailDimStore'  -- Package Name
UNION ALL

-- Fact Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Fact A'  -- Package Execution Group
WHERE p.PackageName = 'RetailFactSales'  -- Package Name
UNION ALL

-- Tabular Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Tabular'  -- Package Execution Group
WHERE p.PackageName = 'RetailProcess'  -- Package Name
UNION ALL

-- Test Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Test'  -- Package Execution Group
WHERE p.PackageName = 'TestStatic'  -- Package Name
UNION ALL

SELECT p.PackageKey, peg.PackageExecutionGroupKey,20,1,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Test'  -- Package Execution Group
WHERE p.PackageName = 'FrameworkProcess'  -- Package Name
UNION ALL

-- Maintenance Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,0,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Maintenance'  -- Package Execution Group
WHERE p.PackageName = 'UtlMaintenance'  -- Package Name
UNION ALL

-- Subscription Packages

SELECT p.PackageKey, peg.PackageExecutionGroupKey,10,0,0,GetDate(),NULL
FROM Package p
	 INNER JOIN PackageExecutionGroup peg
		ON peg.PackageExecutionGroupName = 'Subscriptions'  -- Package Execution Group
WHERE p.PackageName = 'SubscriptionDaily'  -- Package Name

GO
