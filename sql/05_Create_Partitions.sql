USE K3L_DataMart;
GO

PRINT '';
PRINT 'Implementing Table Partitioning for Fact Tables';
PRINT 'Strategy: Partition by Quarter (DateKey)';
PRINT '';

-- STEP 1: Create Partition Function
PRINT 'Step 1: Creating Partition Function...';
GO

-- Drop if exists
IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_DateKey_Quarterly')
    DROP PARTITION FUNCTION PF_DateKey_Quarterly;
GO

-- Create partition function by quarter
CREATE PARTITION FUNCTION PF_DateKey_Quarterly (INT)
AS RANGE RIGHT FOR VALUES (
    -- 2023
    20230101, 20230401, 20230701, 20231001,
    -- 2024
    20240101, 20240401, 20240701, 20241001,
    -- 2025
    20250101, 20250401, 20250701, 20251001,
    -- 2026
    20260101, 20260401, 20260701, 20261001,
    -- 2027
    20270101, 20270401, 20270701, 20271001,
    -- 2028
    20280101, 20280401, 20280701, 20281001,
    -- 2029
    20290101, 20290401, 20290701, 20291001,
    -- 2030
    20300101, 20300401, 20300701, 20301001
);
GO

PRINT 'Partition Function created successfully.';
PRINT 'Total partitions: 33 (8 years x 4 quarters + 1)';
GO

-- STEP 2: Create Partition Scheme
PRINT 'Step 2: Creating Partition Scheme...';
GO

-- Drop if exists
IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_DateKey_Quarterly')
    DROP PARTITION SCHEME PS_DateKey_Quarterly;
GO

-- Create partition scheme (all partitions to PRIMARY filegroup for simplicity)
CREATE PARTITION SCHEME PS_DateKey_Quarterly
AS PARTITION PF_DateKey_Quarterly
ALL TO ([PRIMARY]);
GO

PRINT 'Partition Scheme created successfully.';
PRINT 'All partitions mapped to PRIMARY filegroup.';
GO


-- STEP 3: Drop Existing Fact Tables
PRINT '';
PRINT 'Step 3: Dropping existing Fact Tables...';
PRINT 'WARNING: This will delete all data in fact tables!';
GO

-- Drop existing fact tables (with their constraints)
IF OBJECT_ID('dbo.Fact_Insiden', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Fact_Insiden;
    PRINT 'Fact_Insiden dropped.';
END
GO

IF OBJECT_ID('dbo.Fact_Inspeksi', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Fact_Inspeksi;
    PRINT 'Fact_Inspeksi dropped.';
END
GO

IF OBJECT_ID('dbo.Fact_Limbah', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Fact_Limbah;
    PRINT 'Fact_Limbah dropped.';
END
GO

PRINT 'All fact tables dropped successfully.';
GO


-- STEP 4: Recreate Fact Tables with Partitioning
PRINT '';
PRINT 'Step 4: Recreating Fact Tables with Partitioning...';
GO


-- Fact Table 1: Fact_Insiden (Partitioned)
PRINT 'Creating partitioned Fact_Insiden...';
GO

CREATE TABLE dbo.Fact_Insiden
(
    -- Surrogate Key
    InsidenKey INT IDENTITY(1,1) NOT NULL,
    
    -- Foreign Keys (Dimension References)
    DateKey INT NOT NULL,
    LokasiKey INT NOT NULL,
    UnitKerjaKey INT NOT NULL,
    JenisInsidenKey INT NOT NULL,
    KeparahanKey INT NOT NULL,
    
    -- Degenerate Dimension (Transaction ID)
    LaporanID NVARCHAR(50) NOT NULL,
    
    -- Facts (Measures)
    JumlahKerugian DECIMAL(18, 2) NOT NULL DEFAULT 0,
    JmlHariHilang INT NOT NULL DEFAULT 0,
    JmlKorban INT NOT NULL DEFAULT 0,
    
    -- Constraints
    CONSTRAINT PK_Fact_Insiden PRIMARY KEY CLUSTERED (InsidenKey, DateKey), -- Include DateKey in PK for partitioning
    CONSTRAINT FK_Fact_Insiden_Date FOREIGN KEY (DateKey) 
        REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Insiden_Lokasi FOREIGN KEY (LokasiKey) 
        REFERENCES dbo.Dim_Lokasi(LokasiKey),
    CONSTRAINT FK_Fact_Insiden_UnitKerja FOREIGN KEY (UnitKerjaKey) 
        REFERENCES dbo.Dim_UnitKerja(UnitKerjaKey),
    CONSTRAINT FK_Fact_Insiden_JenisInsiden FOREIGN KEY (JenisInsidenKey) 
        REFERENCES dbo.Dim_JenisInsiden(JenisInsidenKey),
    CONSTRAINT FK_Fact_Insiden_Keparahan FOREIGN KEY (KeparahanKey) 
        REFERENCES dbo.Dim_TingkatKeparahan(KeparahanKey),
    
    -- Check Constraints
    CONSTRAINT CHK_Fact_Insiden_JumlahKerugian CHECK (JumlahKerugian >= 0),
    CONSTRAINT CHK_Fact_Insiden_JmlHariHilang CHECK (JmlHariHilang >= 0),
    CONSTRAINT CHK_Fact_Insiden_JmlKorban CHECK (JmlKorban >= 0)
)
ON PS_DateKey_Quarterly(DateKey); -- Partition on DateKey
GO

PRINT 'Fact_Insiden created with partitioning.';
GO

-- Fact Table 2: Fact_Inspeksi (Partitioned)
PRINT 'Creating partitioned Fact_Inspeksi...';
GO

CREATE TABLE dbo.Fact_Inspeksi
(
    -- Surrogate Key
    InspeksiKey INT IDENTITY(1,1) NOT NULL,
    
    -- Foreign Keys (Dimension References)
    DateKey INT NOT NULL,
    LokasiKey INT NOT NULL,
    PeralatanKey INT NOT NULL,
    
    -- Degenerate Dimension
    InspeksiID NVARCHAR(50) NOT NULL,
    
    -- Facts (Measures)
    JmlTemuan INT NOT NULL DEFAULT 0,
    StatusKepatuhan BIT NOT NULL DEFAULT 1, -- 1 = Compliant, 0 = Non-Compliant
    DurasiTemuanTerbuka INT NOT NULL DEFAULT 0, -- Days
    
    -- Constraints
    CONSTRAINT PK_Fact_Inspeksi PRIMARY KEY CLUSTERED (InspeksiKey, DateKey), -- Include DateKey in PK
    CONSTRAINT FK_Fact_Inspeksi_Date FOREIGN KEY (DateKey) 
        REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Inspeksi_Lokasi FOREIGN KEY (LokasiKey) 
        REFERENCES dbo.Dim_Lokasi(LokasiKey),
    CONSTRAINT FK_Fact_Inspeksi_Peralatan FOREIGN KEY (PeralatanKey) 
        REFERENCES dbo.Dim_Peralatan(PeralatanKey),
    
    -- Check Constraints
    CONSTRAINT CHK_Fact_Inspeksi_JmlTemuan CHECK (JmlTemuan >= 0),
    CONSTRAINT CHK_Fact_Inspeksi_DurasiTemuanTerbuka CHECK (DurasiTemuanTerbuka >= 0)
)
ON PS_DateKey_Quarterly(DateKey); -- Partition on DateKey
GO

PRINT 'Fact_Inspeksi created with partitioning.';
GO


-- Fact Table 3: Fact_Limbah (Partitioned)
PRINT 'Creating partitioned Fact_Limbah...';
GO

CREATE TABLE dbo.Fact_Limbah
(
    -- Surrogate Key
    LimbahKey INT IDENTITY(1,1) NOT NULL,
    
    -- Foreign Keys (Dimension References)
    DateKey INT NOT NULL,
    UnitKerjaKey INT NOT NULL,
    JenisLimbahKey INT NOT NULL,
    
    -- Degenerate Dimension
    TransaksiID NVARCHAR(50) NOT NULL,
    
    -- Facts (Measures)
    VolumeKg DECIMAL(18, 3) NOT NULL DEFAULT 0, -- Weight in Kilograms
    BiayaPengelolaan DECIMAL(18, 2) NOT NULL DEFAULT 0, -- Cost in IDR
    
    -- Constraints
    CONSTRAINT PK_Fact_Limbah PRIMARY KEY CLUSTERED (LimbahKey, DateKey), -- Include DateKey in PK
    CONSTRAINT FK_Fact_Limbah_Date FOREIGN KEY (DateKey) 
        REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Limbah_UnitKerja FOREIGN KEY (UnitKerjaKey) 
        REFERENCES dbo.Dim_UnitKerja(UnitKerjaKey),
    CONSTRAINT FK_Fact_Limbah_JenisLimbah FOREIGN KEY (JenisLimbahKey) 
        REFERENCES dbo.Dim_JenisLimbah(JenisLimbahKey),
    
    -- Check Constraints
    CONSTRAINT CHK_Fact_Limbah_VolumeKg CHECK (VolumeKg >= 0),
    CONSTRAINT CHK_Fact_Limbah_BiayaPengelolaan CHECK (BiayaPengelolaan >= 0)
)
ON PS_DateKey_Quarterly(DateKey); -- Partition on DateKey
GO

PRINT 'Fact_Limbah created with partitioning.';
GO

PRINT '';
PRINT 'All fact tables recreated with partitioning successfully!';
GO


-- STEP 5: Verification & Information
PRINT '';
PRINT 'Partition Implementation Summary';
PRINT '';

-- Check partition function
SELECT 
    pf.name AS PartitionFunction,
    pf.type_desc AS RangeType,
    pf.fanout AS PartitionCount,
    pf.boundary_value_on_right AS BoundaryType
FROM 
    sys.partition_functions pf
WHERE 
    pf.name = 'PF_DateKey_Quarterly';
GO

-- Check partition scheme
SELECT 
    ps.name AS PartitionScheme,
    pf.name AS PartitionFunction,
    ds.name AS Filegroup
FROM 
    sys.partition_schemes ps
    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
    INNER JOIN sys.data_spaces ds ON dds.data_space_id = ds.data_space_id
WHERE 
    ps.name = 'PS_DateKey_Quarterly'
GROUP BY 
    ps.name, pf.name, ds.name;
GO

-- Check partitioned tables
SELECT 
    OBJECT_NAME(p.object_id) AS TableName,
    i.name AS IndexName,
    p.partition_number AS PartitionNumber,
    p.rows AS RowCount,
    prv.value AS BoundaryValue
FROM 
    sys.partitions p
    INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    LEFT JOIN sys.partition_range_values prv ON p.partition_id = prv.boundary_id
WHERE 
    OBJECT_NAME(p.object_id) IN ('Fact_Insiden', 'Fact_Inspeksi', 'Fact_Limbah')
    AND i.type <= 1 -- Clustered index only
ORDER BY 
    OBJECT_NAME(p.object_id), p.partition_number;
GO

-- List boundary values with quarter information
SELECT 
    prv.boundary_id AS PartitionID,
    prv.value AS BoundaryValue,
    CAST(CAST(prv.value AS VARCHAR(8)) AS DATE) AS BoundaryDate,
    YEAR(CAST(CAST(prv.value AS VARCHAR(8)) AS DATE)) AS Year,
    DATEPART(QUARTER, CAST(CAST(prv.value AS VARCHAR(8)) AS DATE)) AS Quarter
FROM 
    sys.partition_range_values prv
    INNER JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
WHERE 
    pf.name = 'PF_DateKey_Quarterly'
ORDER BY 
    prv.value;
GO

PRINT '';
PRINT 'Partitioning Implementation Completed!';
PRINT '';
GO
