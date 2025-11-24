USE K3L_DataMart;
GO

PRINT '';
PRINT 'Creating Staging Tables for K3L Data Mart';
PRINT '';


-- SECTION 1: DROP EXISTING STAGING TABLES
PRINT 'Dropping existing staging tables if they exist...';
GO

IF OBJECT_ID('dbo.STG_Lokasi', 'U') IS NOT NULL
    DROP TABLE dbo.STG_Lokasi;
GO

IF OBJECT_ID('dbo.STG_UnitKerja', 'U') IS NOT NULL
    DROP TABLE dbo.STG_UnitKerja;
GO

IF OBJECT_ID('dbo.STG_Peralatan', 'U') IS NOT NULL
    DROP TABLE dbo.STG_Peralatan;
GO

IF OBJECT_ID('dbo.STG_Insiden', 'U') IS NOT NULL
    DROP TABLE dbo.STG_Insiden;
GO

IF OBJECT_ID('dbo.STG_Inspeksi', 'U') IS NOT NULL
    DROP TABLE dbo.STG_Inspeksi;
GO

IF OBJECT_ID('dbo.STG_Limbah', 'U') IS NOT NULL
    DROP TABLE dbo.STG_Limbah;
GO

PRINT 'Existing staging tables dropped (if any).';
GO


-- SECTION 2: CREATE STAGING TABLES FOR DIMENSIONS
PRINT '';
PRINT 'Creating staging tables for Dimensions...';
GO

-- STG_Lokasi (for Dim_Lokasi)
PRINT 'Creating STG_Lokasi...';
GO

CREATE TABLE dbo.STG_Lokasi
(
    -- Source columns (all NVARCHAR for flexibility)
    KodeLokasi NVARCHAR(50) NULL,
    NamaGedung NVARCHAR(200) NULL,
    Lantai NVARCHAR(50) NULL,
    NamaRuangan NVARCHAR(200) NULL,
    Kapasitas NVARCHAR(50) NULL,
    LuasM2 NVARCHAR(50) NULL,
    Status NVARCHAR(50) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_Lokasi created successfully.';
GO


-- STG_UnitKerja (for Dim_UnitKerja)
PRINT 'Creating STG_UnitKerja...';
GO

CREATE TABLE dbo.STG_UnitKerja
(
    -- Source columns (all NVARCHAR for flexibility)
    KodeUnit NVARCHAR(50) NULL,
    NamaUnit NVARCHAR(200) NULL,
    Kategori NVARCHAR(100) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_UnitKerja created successfully.';
GO


-- STG_Peralatan (for Dim_Peralatan)
PRINT 'Creating STG_Peralatan...';
GO

CREATE TABLE dbo.STG_Peralatan
(
    -- Source columns (all NVARCHAR for flexibility)
    NoInventaris NVARCHAR(100) NULL,
    JenisPeralatan NVARCHAR(200) NULL,
    Merek NVARCHAR(200) NULL,
    Model NVARCHAR(200) NULL,
    TglPemasangan NVARCHAR(50) NULL,
    TglKadaluarsa NVARCHAR(50) NULL,
    Status NVARCHAR(50) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_Peralatan created successfully.';
GO

PRINT '';
PRINT 'All staging tables for dimensions created successfully!';
GO

-- SECTION 3: CREATE STAGING TABLES FOR FACTS
PRINT '';
PRINT 'Creating staging tables for Facts...';
GO


-- STG_Insiden (for Fact_Insiden)
PRINT 'Creating STG_Insiden...';
GO

CREATE TABLE dbo.STG_Insiden
(
    -- Source columns (all NVARCHAR for flexibility)
    LaporanID NVARCHAR(100) NULL,
    TanggalKejadian NVARCHAR(50) NULL,
    KodeLokasi NVARCHAR(50) NULL,
    KodeUnit NVARCHAR(50) NULL,
    JenisInsiden NVARCHAR(200) NULL,
    TingkatKeparahan NVARCHAR(100) NULL,
    JumlahKerugian NVARCHAR(50) NULL,
    JmlHariHilang NVARCHAR(50) NULL,
    JmlKorban NVARCHAR(50) NULL,
    Deskripsi NVARCHAR(MAX) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_Insiden created successfully.';
GO

-- STG_Inspeksi (for Fact_Inspeksi)
PRINT 'Creating STG_Inspeksi...';
GO

CREATE TABLE dbo.STG_Inspeksi
(
    -- Source columns (all NVARCHAR for flexibility)
    InspeksiID NVARCHAR(100) NULL,
    TanggalInspeksi NVARCHAR(50) NULL,
    KodeLokasi NVARCHAR(50) NULL,
    NoInventaris NVARCHAR(100) NULL,
    JmlTemuan NVARCHAR(50) NULL,
    StatusKepatuhan NVARCHAR(50) NULL,
    DurasiTemuanTerbuka NVARCHAR(50) NULL,
    Keterangan NVARCHAR(MAX) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_Inspeksi created successfully.';
GO


-- STG_Limbah (for Fact_Limbah)
PRINT 'Creating STG_Limbah...';
GO

CREATE TABLE dbo.STG_Limbah
(
    -- Source columns (all NVARCHAR for flexibility)
    TransaksiID NVARCHAR(100) NULL,
    TanggalCatat NVARCHAR(50) NULL,
    KodeUnit NVARCHAR(50) NULL,
    KodeLimbah NVARCHAR(50) NULL,
    VolumeKg NVARCHAR(50) NULL,
    BiayaPengelolaan NVARCHAR(50) NULL,
    Keterangan NVARCHAR(MAX) NULL,
    
    -- ETL Control Columns
    ETL_FileName NVARCHAR(255) NULL,
    ETL_InsertDate DATETIME NOT NULL DEFAULT GETDATE(),
    ETL_ProcessedFlag BIT NOT NULL DEFAULT 0,
    ETL_ErrorMessage NVARCHAR(MAX) NULL,
    
    -- Identity for tracking
    STG_ID INT IDENTITY(1,1) PRIMARY KEY
);
GO

PRINT 'STG_Limbah created successfully.';
GO

PRINT '';
PRINT 'All staging tables for facts created successfully!';
GO


-- SECTION 4: CREATE INDEXES ON STAGING TABLES
PRINT '';
PRINT 'Creating minimal indexes on staging tables for lookup performance...';
GO

-- Indexes for dimension staging (natural keys)
CREATE NONCLUSTERED INDEX IX_STG_Lokasi_KodeLokasi 
    ON dbo.STG_Lokasi(KodeLokasi);
GO

CREATE NONCLUSTERED INDEX IX_STG_UnitKerja_KodeUnit 
    ON dbo.STG_UnitKerja(KodeUnit);
GO

CREATE NONCLUSTERED INDEX IX_STG_Peralatan_NoInventaris 
    ON dbo.STG_Peralatan(NoInventaris);
GO

-- Indexes for fact staging (degenerate dimensions)
CREATE NONCLUSTERED INDEX IX_STG_Insiden_LaporanID 
    ON dbo.STG_Insiden(LaporanID);
GO

CREATE NONCLUSTERED INDEX IX_STG_Inspeksi_InspeksiID 
    ON dbo.STG_Inspeksi(InspeksiID);
GO

CREATE NONCLUSTERED INDEX IX_STG_Limbah_TransaksiID 
    ON dbo.STG_Limbah(TransaksiID);
GO

-- Indexes for ETL control
CREATE NONCLUSTERED INDEX IX_STG_Lokasi_ProcessedFlag 
    ON dbo.STG_Lokasi(ETL_ProcessedFlag);
GO

CREATE NONCLUSTERED INDEX IX_STG_UnitKerja_ProcessedFlag 
    ON dbo.STG_UnitKerja(ETL_ProcessedFlag);
GO

CREATE NONCLUSTERED INDEX IX_STG_Peralatan_ProcessedFlag 
    ON dbo.STG_Peralatan(ETL_ProcessedFlag);
GO

CREATE NONCLUSTERED INDEX IX_STG_Insiden_ProcessedFlag 
    ON dbo.STG_Insiden(ETL_ProcessedFlag);
GO

CREATE NONCLUSTERED INDEX IX_STG_Inspeksi_ProcessedFlag 
    ON dbo.STG_Inspeksi(ETL_ProcessedFlag);
GO

CREATE NONCLUSTERED INDEX IX_STG_Limbah_ProcessedFlag 
    ON dbo.STG_Limbah(ETL_ProcessedFlag);
GO

PRINT 'Indexes on staging tables created successfully.';
GO


-- SECTION 5: CREATE UTILITY STORED PROCEDURES
PRINT '';
PRINT 'Creating utility stored procedures for staging management...';
GO


-- Procedure: Truncate All Staging Tables
IF OBJECT_ID('dbo.usp_Truncate_AllStaging', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Truncate_AllStaging;
GO

CREATE PROCEDURE dbo.usp_Truncate_AllStaging
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        PRINT 'Truncating all staging tables...';
        
        TRUNCATE TABLE dbo.STG_Lokasi;
        PRINT 'STG_Lokasi truncated.';
        
        TRUNCATE TABLE dbo.STG_UnitKerja;
        PRINT 'STG_UnitKerja truncated.';
        
        TRUNCATE TABLE dbo.STG_Peralatan;
        PRINT 'STG_Peralatan truncated.';
        
        TRUNCATE TABLE dbo.STG_Insiden;
        PRINT 'STG_Insiden truncated.';
        
        TRUNCATE TABLE dbo.STG_Inspeksi;
        PRINT 'STG_Inspeksi truncated.';
        
        TRUNCATE TABLE dbo.STG_Limbah;
        PRINT 'STG_Limbah truncated.';
        
        PRINT 'All staging tables truncated successfully.';
        RETURN 0;
    END TRY
    BEGIN CATCH
        PRINT 'Error truncating staging tables: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'Procedure usp_Truncate_AllStaging created.';
GO


-- Procedure: Get Staging Statistics
IF OBJECT_ID('dbo.usp_Get_StagingStatistics', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Get_StagingStatistics;
GO

CREATE PROCEDURE dbo.usp_Get_StagingStatistics
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        'STG_Lokasi' AS TableName,
        COUNT(*) AS TotalRows,
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END) AS UnprocessedRows,
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END) AS ProcessedRows,
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END) AS ErrorRows
    FROM dbo.STG_Lokasi
    
    UNION ALL
    
    SELECT 
        'STG_UnitKerja',
        COUNT(*),
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END)
    FROM dbo.STG_UnitKerja
    
    UNION ALL
    
    SELECT 
        'STG_Peralatan',
        COUNT(*),
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END)
    FROM dbo.STG_Peralatan
    
    UNION ALL
    
    SELECT 
        'STG_Insiden',
        COUNT(*),
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END)
    FROM dbo.STG_Insiden
    
    UNION ALL
    
    SELECT 
        'STG_Inspeksi',
        COUNT(*),
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END)
    FROM dbo.STG_Inspeksi
    
    UNION ALL
    
    SELECT 
        'STG_Limbah',
        COUNT(*),
        SUM(CASE WHEN ETL_ProcessedFlag = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ProcessedFlag = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ETL_ErrorMessage IS NOT NULL THEN 1 ELSE 0 END)
    FROM dbo.STG_Limbah
    
    ORDER BY TableName;
END
GO

PRINT 'Procedure usp_Get_StagingStatistics created.';
GO

-- Procedure: Clear Processed Records
IF OBJECT_ID('dbo.usp_Clear_ProcessedStaging', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Clear_ProcessedStaging;
GO

CREATE PROCEDURE dbo.usp_Clear_ProcessedStaging
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @DeletedRows INT = 0;
    
    BEGIN TRY
        PRINT 'Deleting processed records from staging tables...';
        
        DELETE FROM dbo.STG_Lokasi WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        DELETE FROM dbo.STG_UnitKerja WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        DELETE FROM dbo.STG_Peralatan WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        DELETE FROM dbo.STG_Insiden WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        DELETE FROM dbo.STG_Inspeksi WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        DELETE FROM dbo.STG_Limbah WHERE ETL_ProcessedFlag = 1;
        SET @DeletedRows = @DeletedRows + @@ROWCOUNT;
        
        PRINT 'Total processed records deleted: ' + CAST(@DeletedRows AS VARCHAR(10));
        RETURN 0;
    END TRY
    BEGIN CATCH
        PRINT 'Error deleting processed records: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'Procedure usp_Clear_ProcessedStaging created.';
GO

PRINT '';
PRINT 'All utility procedures created successfully!';
GO


-- SECTION 6: VERIFICATION & SUMMARY
PRINT '';
PRINT 'Staging Tables Creation Summary';
PRINT '';

-- List all staging tables
SELECT 
    name AS TableName,
    create_date AS CreatedDate
FROM 
    sys.tables
WHERE 
    name LIKE 'STG_%'
ORDER BY 
    name;
GO

-- Get column counts
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    COUNT(*) AS ColumnCount
FROM 
    sys.columns
WHERE 
    OBJECT_NAME(object_id) LIKE 'STG_%'
GROUP BY 
    OBJECT_NAME(object_id)
ORDER BY 
    OBJECT_NAME(object_id);
GO

-- List utility procedures
SELECT 
    name AS ProcedureName,
    create_date AS CreatedDate
FROM 
    sys.procedures
WHERE 
    name LIKE 'usp_%Staging%'
ORDER BY 
    name;
GO

PRINT '';
PRINT 'Staging Tables Setup Completed!';
PRINT '';
GO
