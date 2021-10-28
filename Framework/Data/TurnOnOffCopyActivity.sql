/****** Script for SelectTopNRows command from SSMS  ******/
SELECT copyactivitysinkname
      ,[CopyActivityExecutionGroupKey]
      ,[ContainerKey]
      ,[CopyActivityOrder]
      ,p.[IsActive]
      ,[IsEmailEnabled]
      ,[IsRestart]
      ,[CreatedDate]
  FROM [dbo].[CopyActivityExecutionPlan] p
  JOIN CopyActivitySink s
  on p.CopyActivitySinkKey = s.CopyActivitySinkKey
  where CopyActivitySinkName  like
  '%AX_PurchTable%'

  Update [CopyActivityExecutionPlan]
  set isActive = 0

  Update [CopyActivityExecutionPlan]
  set isActive = 1
  FROM [dbo].[CopyActivityExecutionPlan] p
  JOIN CopyActivitySink s
  on p.CopyActivitySinkKey = s.CopyActivitySinkKey
  where CopyActivitySinkName like
  '%AX_PurchTable%'
   
  Update [CopyActivityExecutionPlan]
  set isActive = 1

