MERGE dbo.CGS_CopyActivityExcelSheet AS ex
USING ( 
VALUES 
	('BIDataSources_ProForma_Data', 'Pro Forma Data Ready To Load'),
	('BIDataSources_ProForma_Ramp_Up', 'Ramp up ready to load'),
	('BIDataSources_ProForma_Seasonality_Factors', 'Seasonality Ready to load'),
	('BIDataSources_FuelGoals_PL_Monthly', 'PL_Monthly'),
	('BIDataSources_FuelGoals_PL_Quarterly', 'PL_Quarterly'),
	('BIDataSources_FuelGoals_PL_Yearly', 'PL_Yearly'),
	('BIDataSources_FuelGoals_BU_Monthly', 'BU_Monthly'),
	('BIDataSources_FuelGoals_BU_Quarterly', 'BU_Quarterly'),
	('BIDataSources_FuelGoals_BU_Yearly', 'BU_Yearly'),
	('ElnasaMFT_SecretShopper', 'Sheet1')
) AS Sheets
	(
		CopyActivitySinkName,
		Sheet_Name
	)
ON ex.CopyActivitySinkName = Sheets.CopyActivitySinkName
WHEN MATCHED THEN
	UPDATE
	SET Sheet_Name = Sheets.Sheet_Name,
		ModifiedDate = CAST(GETDATE() AS DATETIME2(3))
WHEN NOT MATCHED BY TARGET THEN
	INSERT (
		CopyActivitySinkName,
		Sheet_Name,
		CreatedDate
	)
	VALUES (
		Sheets.CopyActivitySinkName,
		Sheets.Sheet_Name,
		CAST(GETDATE() AS DATETIME2(3))
	);
GO