/*********************************************************************************************************************
	File: 	StoredProcedureParameter.data.sql

	Desc: 	Data hydration script.

	Auth: 	Alan Campbell
	Date: 	09/20/2018

	NOTE:	
         

	==================================================================================================================
    Change History
	==================================================================================================================
	Date		Author						Description
	----------- ---------------------------	--------------------------------------------------------------------------
	09/20/2018	Alan Campbell				Created.
	
**********************************************************************************************************************/

MERGE dbo.StoredProcedureParameter AS t
  USING (
  VALUES 
		 (1,'Mode','Initial',GetDate(),NULL)
		,(2,'Mode','Initial',GetDate(),NULL)
		,(3,'Mode','Initial',GetDate(),NULL)
		,(4,'Mode','Initial',GetDate(),NULL)
		,(5,'Mode','Initial',GetDate(),NULL)
		,(6,'Mode','Initial',GetDate(),NULL)
		,(7,'Mode','Initial',GetDate(),NULL)
		,(8,'Mode','Initial',GetDate(),NULL)
		,(9,'Mode','Initial',GetDate(),NULL)
		,(10,'Mode','Initial',GetDate(),NULL)
		,(11,'Mode','Initial',GetDate(),NULL)
		,(12,'Mode','Initial',GetDate(),NULL)
		,(13,'Mode','Initial',GetDate(),NULL)
		,(14,'Mode','Initial',GetDate(),NULL)
		,(15,'Mode','Initial',GetDate(),NULL)
		,(16,'Mode','Initial',GetDate(),NULL)
		,(17,'Mode','Initial',GetDate(),NULL)
		,(18,'Mode','Initial',GetDate(),NULL)
		,(19,'Mode','Initial',GetDate(),NULL)
		,(20,'Mode','Initial',GetDate(),NULL)
		,(21,'Mode','Initial',GetDate(),NULL)
		,(22,'Mode','Initial',GetDate(),NULL)
		,(23,'Mode','Initial',GetDate(),NULL)
		,(24,'Mode','Initial',GetDate(),NULL)
		,(25,'Mode','Initial',GetDate(),NULL)
		,(26,'Mode','Initial',GetDate(),NULL)
		,(27,'Mode','Initial',GetDate(),NULL)
		,(28,'Mode','Initial',GetDate(),NULL)
		,(29,'Mode','Initial',GetDate(),NULL)
		,(30,'Mode','Initial',GetDate(),NULL)
		,(31,'Mode','Initial',GetDate(),NULL)
		,(32,'Mode','Initial',GetDate(),NULL)
		,(33,'Mode','Initial',GetDate(),NULL)
		,(34,'Mode','Initial',GetDate(),NULL)
		,(35,'Mode','Initial',GetDate(),NULL)
		,(36,'Mode','Initial',GetDate(),NULL)
		,(37,'Mode','Initial',GetDate(),NULL)
		,(38,'Mode','Initial',GetDate(),NULL)
		,(39,'Mode','Initial',GetDate(),NULL)
		,(40,'Mode','Initial',GetDate(),NULL)
		,(41,'Mode','Initial',GetDate(),NULL)
		,(42,'Mode','Initial',GetDate(),NULL)
		,(43,'Mode','Initial',GetDate(),NULL)
		,(44,'Mode','Initial',GetDate(),NULL)
		,(45,'Mode','Initial',GetDate(),NULL)
		,(46,'Mode','Initial',GetDate(),NULL)
		,(47,'Mode','Initial',GetDate(),NULL)
		,(48,'Mode','Initial',GetDate(),NULL)
		,(49,'Mode','Initial',GetDate(),NULL)
		,(50,'Mode','Initial',GetDate(),NULL)
		,(51,'Mode','Initial',GetDate(),NULL)
		,(52,'Mode','Initial',GetDate(),NULL)
		,(53,'Mode','Initial',GetDate(),NULL)
		,(54,'Mode','Initial',GetDate(),NULL)
		,(55,'Mode','Initial',GetDate(),NULL)
		,(56,'Mode','Initial',GetDate(),NULL)
		,(57,'Mode','Initial',GetDate(),NULL)
		,(58,'Mode','Initial',GetDate(),NULL)
		,(59,'Mode','Initial',GetDate(),NULL)
		,(60,'Mode','Initial',GetDate(),NULL)


		 
		 ) as s
		(
			 StoredProcedureKey
			,StoredProcedureParameterName
			,StoredProcedureParameterValue
			,CreatedDate
			,ModifiedDate
		)
ON ( t.StoredProcedureKey = s.StoredProcedureKey AND t.StoredProcedureParameterName = s.StoredProcedureParameterName )
WHEN MATCHED THEN 
	UPDATE SET   StoredProcedureParameterValue = s.StoredProcedureParameterValue
				,CreatedDate = s.CreatedDate
				,ModifiedDate = s.ModifiedDate
WHEN NOT MATCHED BY TARGET THEN
    INSERT(
			 StoredProcedureKey
			,StoredProcedureParameterName
			,StoredProcedureParameterValue
			,CreatedDate
			,ModifiedDate
		  )	
    VALUES(
			 s.StoredProcedureKey
			,s.StoredProcedureParameterName
			,s.StoredProcedureParameterValue
			,s.CreatedDate
			,s.ModifiedDate
		  );
GO

