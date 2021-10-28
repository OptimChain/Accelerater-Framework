MERGE dbo.CopyActivityParameter AS t
  USING (
  VALUES 
(100,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(101,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(102,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(103,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(104,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(105,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(106,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL),
(107,'dbConnectionSecret','TSAConnectionString',GetDate(),NULL)

		  ) as s
		(
			 CopyActivitySinkKey
			,CopyActivityParameterName
			,CopyActivityParameterValue
			,CreatedDate
			,ModifiedDate
		)
ON ( t.CopyActivitySinkKey = s.CopyActivitySinkKey AND t.CopyActivityParameterName = s.CopyActivityParameterName )
WHEN MATCHED THEN 
	UPDATE SET   CopyActivityParameterValue = s.CopyActivityParameterValue
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 CopyActivitySinkKey
			,CopyActivityParameterName
			,CopyActivityParameterValue
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.CopyActivitySinkKey
			,s.CopyActivityParameterName
			,s.CopyActivityParameterValue
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO
