CREATE DATABASE K3L_DataMart
ON PRIMARY
(
    NAME = 'K3L_DataMart_Data',
    FILENAME = '/var/opt/mssql/data/K3L_DataMart_Data.mdf',
    SIZE = 100MB,
    MAXSIZE = 10GB,
    FILEGROWTH = 50MB
)
LOG ON
(
    NAME = 'K3L_DataMart_Log',
    FILENAME = '/var/opt/mssql/data/K3L_DataMart_Log.ldf',
    SIZE = 50MB,
    MAXSIZE = 5GB,
    FILEGROWTH = 25MB
);
GO

PRINT 'Database K3L_DataMart created successfully.';
GO

-- Set database options
ALTER DATABASE K3L_DataMart 
SET RECOVERY SIMPLE;
GO

ALTER DATABASE K3L_DataMart 
SET AUTO_CREATE_STATISTICS ON;
GO

ALTER DATABASE K3L_DataMart 
SET AUTO_UPDATE_STATISTICS ON;
GO

PRINT 'Database configuration completed.';
GO

-- Use the database
USE K3L_DataMart;
GO

PRINT '';
PRINT 'Database K3L_DataMart created successfully!';
GO
