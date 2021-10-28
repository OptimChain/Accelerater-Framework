



/*********************************************************************************************************************
	File: 	StoredProcedure.data.sql

	Desc: 	Data hydration script.

	Auth: 	Joseph Barth
	Date: 	08/16/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	08/16/2018	Joseph Barth				Created.
	09/18/2018	Alan Campbell				Added some additional stored procedures that are design to handle initial 
	                                        or incremental loads based on a passed parameter.
**********************************************************************************************************************/

SET IDENTITY_INSERT dbo.StoredProcedure ON;

MERGE dbo.StoredProcedure AS t
  USING (
  VALUES 
		 (1,'dbo.uspDimCompany',NULL,NULL,GetDate(),NULL)
		,(2,'dbo.uspDimDate',NULL,NULL,GetDate(),NULL)
		,(3,'dbo.uspDimState',NULL,NULL,GetDate(),NULL)
		,(4,'dbo.uspDimSystem',NULL,NULL,GetDate(),NULL)
		,(5,'dbo.uspDimEmployee',NULL,NULL,GetDate(),NULL)
		,(6,'GPAmus.uspDimSite',NULL,NULL,GetDate(),NULL)
		,(7,'ComsMillsEntOps.uspDimBasis',NULL,NULL,GetDate(),NULL)
		,(8,'PrismBinService.uspDimBin',NULL,NULL,GetDate(),NULL)
		,(9,'ComsMillsEntOps.uspDimCommodityMarket',NULL,NULL,GetDate(),NULL)
		,(10,'GPAmus.uspDimCustomer',NULL,NULL,GetDate(),NULL)
		,(11,'PrismPackFactorService.uspDimGrainType',NULL,NULL,GetDate(),NULL)
		,(12,'PrismDowntimeService.uspDimMillDowntimeReason',NULL,NULL,GetDate(),NULL)
		,(13,'PrismMillingMasterDataService.uspDimMillUnit',NULL,NULL,GetDate(),NULL)
		,(14,'PrismDowntimeService.uspDimPackDowntimeReason',NULL,NULL,GetDate(),NULL)
		,(15,'PrismPackMasterDataService.uspDimPackLine',NULL,NULL,GetDate(),NULL)
		,(16,'PrismQualityService.uspDimSifter',NULL,NULL,GetDate(),NULL)
		,(17,'PrismGrainMixService.uspDimGrainMixBlend',NULL,NULL,GetDate(),NULL)
		,(18,'GPAmus.uspDimVendor',NULL,NULL,GetDate(),NULL)
		,(19,'PrismMaterialService.uspDimUnitOfMeasureSchedule',NULL,NULL,GetDate(),NULL)
		,(20,'PrismGrainMixService.uspDimGrainMix',NULL,NULL,GetDate(),NULL)
		,(21,'PrismPackFactorService.uspDimGrainPackFactor',NULL,NULL,GetDate(),NULL)
		,(22,'GPAmus.uspDimCustomerAddress',NULL,NULL,GetDate(),NULL)
		,(23,'ComsMillsEntOps.uspDimItemFeedXref',NULL,NULL,GetDate(),NULL)
		,(24,'ComsMillsEntOps.uspDimItemMixXref',NULL,NULL,GetDate(),NULL)
		,(25,'PrismMillingMasterDataService.uspDimMillUnitTarget',NULL,NULL,GetDate(),NULL)
		,(26,'PrismMaterialService.uspDimUnitOfMeasureConversion',NULL,NULL,GetDate(),NULL)
		,(27,'GPAmus.uspDimVendorAddress',NULL,NULL,GetDate(),NULL)
		,(28,'PrismMaterialService.uspDimItem',NULL,NULL,GetDate(),NULL)
		,(29,'PrismGrainMixService.uspDimGrainMixBlendComponent',NULL,NULL,GetDate(),NULL)
		,(30,'ComsMillsEntOps.uspFactItemMix',NULL,NULL,GetDate(),NULL)
		,(31,'PrismPackingService.uspFactLot',NULL,NULL,GetDate(),NULL)
		,(32,'ComsMillsEntOps.uspFactMarkToMarket',NULL,NULL,GetDate(),NULL)
		,(33,'ComsMillsEntOps.uspFactMarkToMarketRange',NULL,NULL,GetDate(),NULL)
		,(34,'PrismProductionRunService.uspFactMillProductionRun',NULL,NULL,GetDate(),NULL)
		,(35,'PrismProductionRunService.uspFactMillProductionQualitySample',NULL,NULL,GetDate(),NULL)
		,(36,'PrismSchedulingService.uspFactMillUnitSchedule',NULL,NULL,GetDate(),NULL)
		,(37,'PrismOrderService.uspFactOrder',NULL,NULL,GetDate(),NULL)
		,(38,'PrismTemperTransferService.uspFactTemperTransferProductionRun',NULL,NULL,GetDate(),NULL)
		,(39,'PrismPackingService.uspFactPackProductionRun',NULL,NULL,GetDate(),NULL)
		,(40,'PrismProductionRunService.uspFactMillProductionTemperBin',NULL,NULL,GetDate(),NULL)
		,(41,'PrismProductionRunService.uspFactMillProductionTemperTransfer',NULL,NULL,GetDate(),NULL)
		,(42,'PrismProductionRunService.uspFactMillProductionQualityReading',NULL,NULL,GetDate(),NULL)
		,(43,'PrismProductionRunService.uspFactMillProductionBin',NULL,NULL,GetDate(),NULL)
		,(44,'PrismProductionRunService.uspFactMillProductionDowntime',NULL,NULL,GetDate(),NULL)
		,(45,'PrismShippingService.uspFactLoadOut',NULL,NULL,GetDate(),NULL)
		,(46,'PrismShippingService.uspFactLoadOutProduct',NULL,NULL,GetDate(),NULL)
		,(47,'PrismSchedulingService.uspFactBulkSchedule',NULL,NULL,GetDate(),NULL)
		,(48,'PrismTemperTransferService.uspFactTemperTransferProductionBin',NULL,NULL,GetDate(),NULL)
		,(49,'PrismTemperTransferService.uspFactTemperTransferProductionSample',NULL,NULL,GetDate(),NULL)
		,(50,'PrismShippingService.uspFactLoadOutSeal',NULL,NULL,GetDate(),NULL)
		,(51,'PrismQualityService.uspFactTailings',NULL,NULL,GetDate(),NULL)
		,(52,'PrismQualityService.uspFactCOA',NULL,NULL,GetDate(),NULL)
		,(53,'PrismPackingService.uspFactEmptyBagLotTracking',NULL,NULL,GetDate(),NULL)
		,(54,'PrismShippingService.uspFactLoadOutProductLot',NULL,NULL,GetDate(),NULL)
		,(55,'PrismPackingService.uspFactPackProductionDowntime',NULL,NULL,GetDate(),NULL)
		,(56,'PrismPackingService.uspFactPackProductionBin',NULL,NULL,GetDate(),NULL)
		,(57,'PrismSchedulingService.uspFactPackSchedule',NULL,NULL,GetDate(),NULL)
		,(58,'ComsMillsEntOps.uspFactFuturesFreight',NULL,NULL,GetDate(),NULL)
		,(59,'ComsMillsEntOps.uspFactFutures',NULL,NULL,GetDate(),NULL)
		,(60,'ComsMillsEntOps.uspDimCompanyLocation',NULL,NULL,GetDate(),NULL)

		 ) as s
		(
			 StoredProcedureKey
			,StoredProcedureName
			,StoredProcedureCreationDate
			,StoredProcedureCreatedBy
			,CreatedDate
			,ModifiedDate
		)
ON ( t.StoredProcedureKey = s.StoredProcedureKey )
WHEN MATCHED THEN 
	UPDATE SET   StoredProcedureName = s.StoredProcedureName
				,StoredProcedureCreationDate = s.StoredProcedureCreationDate
				,StoredProcedureCreatedBy = s.StoredProcedureCreatedBy
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 StoredProcedureKey
			,StoredProcedureName
			,StoredProcedureCreationDate
			,StoredProcedureCreatedBy
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.StoredProcedureKey
			,s.StoredProcedureName
			,s.StoredProcedureCreationDate
			,s.StoredProcedureCreatedBy
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

SET IDENTITY_INSERT dbo.StoredProcedure OFF;
