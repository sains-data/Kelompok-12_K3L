/*********************************************************
SECURITY IMPLEMENTATION — K3L_DataMart
*********************************************************/
USE K3L_DataMart;
GO

/*========================================================
1.1. CREATE USER ROLES
========================================================*/

-- Create Roles
CREATE ROLE db_executive;
CREATE ROLE db_analyst;
CREATE ROLE db_viewer;
CREATE ROLE db_etl_operator;
GO

---

-- Executive Permissions (Full Read + ETL)

GRANT SELECT ON SCHEMA::dbo TO db_executive;
GRANT EXECUTE ON SCHEMA::dbo TO db_executive;
GO

---

-- Analyst Permissions (DW + staging jika ada)

GRANT SELECT ON SCHEMA::dbo TO db_analyst;
-- Jika ada schema staging, aktifkan:
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO db_analyst;
GO

---

-- Viewer Permissions (Read-only analytical views)

GRANT SELECT ON dbo.vw_Insiden_Summary        TO db_viewer;
GRANT SELECT ON dbo.vw_Inspeksi_Summary       TO db_viewer;
GRANT SELECT ON dbo.vw_Limbah_Summary         TO db_viewer;
GRANT SELECT ON dbo.vw_Executive_Summary_K3L  TO db_viewer;
GO

---

-- ETL Operator Permissions

GRANT EXECUTE ON SCHEMA::dbo TO db_etl_operator;
GRANT INSERT ON SCHEMA::dbo TO db_etl_operator;
GO

/*========================================================
1.2. CREATE USERS AND ASSIGN ROLES
========================================================*/
USE master;
GO

-- Create SQL Logins (Server-level)
CREATE LOGIN executive_user WITH PASSWORD = 'StrongP@ssw0rd!';
CREATE LOGIN analyst_user   WITH PASSWORD = 'StrongP@ssw0rd!';
CREATE LOGIN viewer_user    WITH PASSWORD = 'StrongP@ssw0rd!';
CREATE LOGIN etl_service    WITH PASSWORD = 'StrongP@ssw0rd!';
GO

-- Create Database Users
USE K3L_DataMart;
GO

CREATE USER executive_user FOR LOGIN executive_user;
CREATE USER analyst_user   FOR LOGIN analyst_user;
CREATE USER viewer_user    FOR LOGIN viewer_user;
CREATE USER etl_service    FOR LOGIN etl_service;
GO

-- Assign Role Memberships
ALTER ROLE db_executive    ADD MEMBER executive_user;
ALTER ROLE db_analyst      ADD MEMBER analyst_user;
ALTER ROLE db_viewer       ADD MEMBER viewer_user;
ALTER ROLE db_etl_operator ADD MEMBER etl_service;
GO

/*===========================================================
1.3 DATA MASKING IMPLEMENTATION
===========================================================*/
-- Create Database Users
USE K3L_DataMart;
GO

CREATE USER executive_user FOR LOGIN executive_user;
CREATE USER analyst_user   FOR LOGIN analyst_user;
CREATE USER viewer_user    FOR LOGIN viewer_user;
CREATE USER etl_service    FOR LOGIN etl_service;
GO

-- Assign Role Memberships
ALTER ROLE db_executive    ADD MEMBER executive_user;
ALTER ROLE db_analyst      ADD MEMBER analyst_user;
ALTER ROLE db_viewer       ADD MEMBER viewer_user;
ALTER ROLE db_etl_operator ADD MEMBER etl_service;
GO



/*===========================================================
    1.4 AUDIT TRAIL IMPLEMENTATION
===========================================================*/
USE K3L_DataMart;
GO
------------------------------------------------------------
-- 1. CREATE AUDIT TABLE
------------------------------------------------------------
IF OBJECT_ID('dbo.AuditLog', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.AuditLog (
        AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
        EventTime DATETIME2 DEFAULT SYSDATETIME(),
        UserName NVARCHAR(128) DEFAULT SUSER_SNAME(),
        EventType NVARCHAR(50),
        SchemaName NVARCHAR(128),
        ObjectName NVARCHAR(128),
        SQLStatement NVARCHAR(MAX) NULL,
        RowsAffected INT,
        HostName NVARCHAR(128) NULL,
        IPAddress VARCHAR(50) NULL,
        ApplicationName NVARCHAR(128) DEFAULT APP_NAME()
    );
END
GO

------------------------------------------------------------
-- 2. TRIGGER: Fact_Insiden
------------------------------------------------------------
CREATE OR ALTER TRIGGER trg_Audit_Fact_Insiden
ON dbo.Fact_Insiden
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventType NVARCHAR(50);

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @EventType = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @EventType = 'INSERT';
    ELSE
        SET @EventType = 'DELETE';

    INSERT INTO dbo.AuditLog (EventType, SchemaName, ObjectName, RowsAffected)
    VALUES (@EventType, 'dbo', 'Fact_Insiden', @@ROWCOUNT);
END;
GO

------------------------------------------------------------
-- 3. TRIGGER: Fact_Inspeksi
------------------------------------------------------------
CREATE OR ALTER TRIGGER trg_Audit_Fact_Inspeksi
ON dbo.Fact_Inspeksi
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventType NVARCHAR(50);

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @EventType = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @EventType = 'INSERT';
    ELSE
        SET @EventType = 'DELETE';

    INSERT INTO dbo.AuditLog (EventType, SchemaName, ObjectName, RowsAffected)
    VALUES (@EventType, 'dbo', 'Fact_Inspeksi', @@ROWCOUNT);
END;
GO

------------------------------------------------------------
-- 4. TRIGGER: Fact_Limbah
------------------------------------------------------------
CREATE OR ALTER TRIGGER trg_Audit_Fact_Limbah
ON dbo.Fact_Limbah
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventType NVARCHAR(50);

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        SET @EventType = 'UPDATE';
    ELSE IF EXISTS (SELECT 1 FROM inserted)
        SET @EventType = 'INSERT';
    ELSE
        SET @EventType = 'DELETE';

    INSERT INTO dbo.AuditLog (EventType, SchemaName, ObjectName, RowsAffected)
    VALUES (@EventType, 'dbo', 'Fact_Limbah', @@ROWCOUNT);
END;
GO

-------------------------------------------------------
-- 5. BUAT SERVER AUDIT 
-------------------------------------------------------
USE master;
GO

CREATE SERVER AUDIT K3L_DataMart_Audit
TO FILE
(
    FILEPATH = N'C:\Audit',
    MAXSIZE = 100 MB,
    MAX_ROLLOVER_FILES = 10
)
WITH (ON_FAILURE = CONTINUE);
GO

-- Aktifkan server audit
ALTER SERVER AUDIT K3L_DataMart_Audit WITH (STATE = ON);
GO

-------------------------------------------------------
-- 6. BUAT DATABASE AUDIT SPECIFICATION
-------------------------------------------------------
USE K3L_DataMart;
GO

CREATE DATABASE AUDIT SPECIFICATION K3L_DB_Audit
FOR SERVER AUDIT K3L_DataMart_Audit
ADD (SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo BY public);
GO

-- Aktifkan DB Audit
ALTER DATABASE AUDIT SPECIFICATION K3L_DB_Audit WITH (STATE = ON);
GO
