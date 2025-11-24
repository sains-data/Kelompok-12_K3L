USE K3L_DataMart;
GO

-- Fact Table 1: Fact_Insiden

IF OBJECT_ID('dbo.Fact_Insiden', 'U') IS NOT NULL
    DROP TABLE dbo.Fact_Insiden;
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
    CONSTRAINT PK_Fact_Insiden PRIMARY KEY CLUSTERED (InsidenKey),
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
);
GO

PRINT 'Table Fact_Insiden created successfully.';
GO

-- Fact Table 2: Fact_Inspeksi

IF OBJECT_ID('dbo.Fact_Inspeksi', 'U') IS NOT NULL
    DROP TABLE dbo.Fact_Inspeksi;
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
    CONSTRAINT PK_Fact_Inspeksi PRIMARY KEY CLUSTERED (InspeksiKey),
    CONSTRAINT FK_Fact_Inspeksi_Date FOREIGN KEY (DateKey) 
        REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Inspeksi_Lokasi FOREIGN KEY (LokasiKey) 
        REFERENCES dbo.Dim_Lokasi(LokasiKey),
    CONSTRAINT FK_Fact_Inspeksi_Peralatan FOREIGN KEY (PeralatanKey) 
        REFERENCES dbo.Dim_Peralatan(PeralatanKey),
    
    -- Check Constraints
    CONSTRAINT CHK_Fact_Inspeksi_JmlTemuan CHECK (JmlTemuan >= 0),
    CONSTRAINT CHK_Fact_Inspeksi_DurasiTemuanTerbuka CHECK (DurasiTemuanTerbuka >= 0)
);
GO

PRINT 'Table Fact_Inspeksi created successfully.';
GO

-- Fact Table 3: Fact_Limbah

IF OBJECT_ID('dbo.Fact_Limbah', 'U') IS NOT NULL
    DROP TABLE dbo.Fact_Limbah;
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
    CONSTRAINT PK_Fact_Limbah PRIMARY KEY CLUSTERED (LimbahKey),
    CONSTRAINT FK_Fact_Limbah_Date FOREIGN KEY (DateKey) 
        REFERENCES dbo.Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Limbah_UnitKerja FOREIGN KEY (UnitKerjaKey) 
        REFERENCES dbo.Dim_UnitKerja(UnitKerjaKey),
    CONSTRAINT FK_Fact_Limbah_JenisLimbah FOREIGN KEY (JenisLimbahKey) 
        REFERENCES dbo.Dim_JenisLimbah(JenisLimbahKey),
    
    -- Check Constraints
    CONSTRAINT CHK_Fact_Limbah_VolumeKg CHECK (VolumeKg >= 0),
    CONSTRAINT CHK_Fact_Limbah_BiayaPengelolaan CHECK (BiayaPengelolaan >= 0)
);
GO

PRINT 'Table Fact_Limbah created successfully.';
GO

PRINT '';
PRINT '==============================================';
PRINT 'All fact tables created successfully!';
PRINT '==============================================';
GO
