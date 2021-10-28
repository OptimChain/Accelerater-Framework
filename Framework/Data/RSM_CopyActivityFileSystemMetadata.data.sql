/*
Calls stored procedure uspInsertFileSystemCopyMetadata hydrating metadata tables for Copy process
*/
--K3S

exec dbo.uspInsertFileSystemCopyMetadata @ContainerName = 'brtl', @CopyActivityExecutionGroupName = 'K3SNoHeader Copy Process' ,@CopyActivityDataName = 'K3S_AssistedOrders', @CopyActivityParameterValue = 'K3SConnectionString', @CopyActivityDataFileSystemFolderPath = '//fv-bd-vfs01p/Integrations/K3SImport/Archive/', @CopyActivityDataFileSystemFileName = 'STR*', @CopyActivityDataADLSFolderPath = '/Raw/K3S/AssistedOrders', @CopyActivityDataADLSFileName = ''
