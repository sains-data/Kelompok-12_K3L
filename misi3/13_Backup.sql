BACKUP DATABASE K3L_DataMart 
TO DISK = N'C:\Backup\K3L_DataMart_Full.bak' 
WITH 
    COMPRESSION, 
    INIT,  
    NAME = N'Full Backup K3L_DataMart', 
    STATS = 10;
GO
BACKUP DATABASE K3L_DataMart
TO DISK = N'C:\Backup\K3L_DataMart_Diff.bak'
WITH 
    DIFFERENTIAL,
    COMPRESSION,
    INIT,
    NAME = N'Differential Backup K3L_DataMart',
    STATS = 10;
GO
BACKUP LOG K3L_DataMart
TO DISK = N'C:\Backup\K3L_DataMart_Log.trn'
WITH 
    COMPRESSION,
    INIT,
    NAME = N'Transaction Log Backup K3L_DataMart',
    STATS = 10;
GO
CREATE CREDENTIAL AzureStorageCredential --(opsional)
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<SAS_TOKEN>';
GO
BACKUP DATABASE K3L_DataMart
TO URL =
N'https://<storage_account>.blob.core.windows.net/backups/K3L_DataMart_FULL.bak'
WITH  
    CREDENTIAL = 'AzureStorageCredential',
    COMPRESSION, 
    STATS = 10;
GO

