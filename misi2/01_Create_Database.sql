-- Create Database
CREATE DATABASE K3L_DataMart
ON PRIMARY
(
    NAME = N'K3L_DataMart_Data',
    FILENAME = N'C:\K3L\K3L_DataMart_Data.mdf',
    SIZE = 1GB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 256MB
)
LOG ON
(
    NAME = N'K3L_DataMart_Log',
    FILENAME = N'C:\K3L\K3L_DataMart_Log.ldf',
    SIZE = 256MB,
    MAXSIZE = 2GB,
    FILEGROWTH = 64MB
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

