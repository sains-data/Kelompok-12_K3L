USE K3L_DataMart;
GO

IF OBJECT_ID('dbo.Dim_Date', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_Date;
GO

CREATE TABLE dbo.Dim_Date
(
    DateKey INT NOT NULL,
    FullDate DATE NOT NULL,
    Hari NVARCHAR(10) NOT NULL,
    NamaHari NVARCHAR(20) NOT NULL,
    HariDalamMinggu INT NOT NULL,
    HariDalamBulan INT NOT NULL,
    HariDalamTahun INT NOT NULL,
    
    Minggu INT NOT NULL,
    MingguDalamTahun INT NOT NULL,
    
    Bulan INT NOT NULL,
    NamaBulan NVARCHAR(20) NOT NULL,
    BulanTahun NVARCHAR(7) NOT NULL, -- Format: 2025-01
    
    Kuartal INT NOT NULL,
    NamaKuartal NVARCHAR(10) NOT NULL, -- Q1, Q2, Q3, Q4
    KuartalTahun NVARCHAR(7) NOT NULL, -- Format: 2025-Q1
    
    Tahun INT NOT NULL,
    
    IsWeekend BIT NOT NULL,
    IsHariLibur BIT NOT NULL DEFAULT 0,
    NamaHariLibur NVARCHAR(100) NULL,
    
    -- Fiscal calendar 
    TahunFiskal INT NULL,
    KuartalFiskal INT NULL,
    
    CONSTRAINT PK_Dim_Date PRIMARY KEY CLUSTERED (DateKey)
);
GO

PRINT 'Table Dim_Date created successfully.';
GO

-- Dimension Table 2: Dim_Lokasi 

IF OBJECT_ID('dbo.Dim_Lokasi', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_Lokasi;
GO

CREATE TABLE dbo.Dim_Lokasi
(
    LokasiKey INT IDENTITY(1,1) NOT NULL,
    KodeLokasi NVARCHAR(20) NOT NULL, -- Natural Key
    NamaGedung NVARCHAR(100) NOT NULL,
    Lantai NVARCHAR(20) NOT NULL,
    NamaRuangan NVARCHAR(100) NOT NULL,
    LokasiLengkap AS (NamaGedung + ' - ' + Lantai + ' - ' + NamaRuangan) PERSISTED,
    
    Kapasitas INT NULL,
    LuasM2 DECIMAL(10,2) NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Aktif', -- Aktif, Non-Aktif
    
    CONSTRAINT PK_Dim_Lokasi PRIMARY KEY CLUSTERED (LokasiKey),
    CONSTRAINT UQ_Dim_Lokasi_KodeLokasi UNIQUE (KodeLokasi)
);
GO

PRINT 'Table Dim_Lokasi created successfully.';
GO

-- Dimension Table 3: Dim_UnitKerja

IF OBJECT_ID('dbo.Dim_UnitKerja', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_UnitKerja;
GO

CREATE TABLE dbo.Dim_UnitKerja
(
    UnitKerjaKey INT IDENTITY(1,1) NOT NULL,
    KodeUnit NVARCHAR(20) NOT NULL, -- Natural Key
    NamaUnit NVARCHAR(100) NOT NULL,
    Kategori NVARCHAR(50) NOT NULL, -- Akademik, Penunjang, Administrasi
    
    CONSTRAINT PK_Dim_UnitKerja PRIMARY KEY CLUSTERED (UnitKerjaKey)
);
GO

PRINT 'Table Dim_UnitKerja created successfully.';
GO


-- Dimension Table 4: Dim_JenisInsiden

IF OBJECT_ID('dbo.Dim_JenisInsiden', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_JenisInsiden;
GO

CREATE TABLE dbo.Dim_JenisInsiden
(
    JenisInsidenKey INT IDENTITY(1,1) NOT NULL,
    NamaJenisInsiden NVARCHAR(100) NOT NULL,
    Kategori NVARCHAR(50) NOT NULL, -- Keselamatan, Lingkungan, Kesehatan
    Deskripsi NVARCHAR(500) NULL,
    CONSTRAINT PK_Dim_JenisInsiden PRIMARY KEY CLUSTERED (JenisInsidenKey)
);
GO

PRINT 'Table Dim_JenisInsiden created successfully.';
GO


-- Dimension Table 5: Dim_TingkatKeparahan

IF OBJECT_ID('dbo.Dim_TingkatKeparahan', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_TingkatKeparahan;
GO

CREATE TABLE dbo.Dim_TingkatKeparahan
(
    KeparahanKey INT IDENTITY(1,1) NOT NULL,
    NamaTingkatKeparahan NVARCHAR(50) NOT NULL,
    Level INT NOT NULL, -- 1-5 (1=Minimal, 5=Fatal)
    Deskripsi NVARCHAR(500) NULL,
    WarnaIndikator NVARCHAR(20) NULL, -- For visualization
    CONSTRAINT PK_Dim_TingkatKeparahan PRIMARY KEY CLUSTERED (KeparahanKey),
    CONSTRAINT CHK_TingkatKeparahan_Level CHECK (Level BETWEEN 1 AND 5)
);
GO

PRINT 'Table Dim_TingkatKeparahan created successfully.';
GO


-- Dimension Table 6: Dim_Peralatan

IF OBJECT_ID('dbo.Dim_Peralatan', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_Peralatan;
GO

CREATE TABLE dbo.Dim_Peralatan
(
    PeralatanKey INT IDENTITY(1,1) NOT NULL,
    NoInventaris NVARCHAR(50) NOT NULL, -- Natural Key
    JenisPeralatan NVARCHAR(100) NOT NULL, -- APAR, Hidran, P3K, Detektor Asap
    Merek NVARCHAR(100) NULL,
    Model NVARCHAR(100) NULL,
    TglPemasangan DATE NULL,
    TglKadaluarsa DATE NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Aktif',
    CONSTRAINT PK_Dim_Peralatan PRIMARY KEY CLUSTERED (PeralatanKey),
    CONSTRAINT UQ_Dim_Peralatan_NoInventaris UNIQUE (NoInventaris)
);
GO

PRINT 'Table Dim_Peralatan created successfully.';
GO


-- Dimension Table 7: Dim_JenisLimbah

IF OBJECT_ID('dbo.Dim_JenisLimbah', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_JenisLimbah;
GO

CREATE TABLE dbo.Dim_JenisLimbah
(
    JenisLimbahKey INT IDENTITY(1,1) NOT NULL,
    KodeLimbah NVARCHAR(20) NOT NULL, -- Natural Key (e.g., B3-001, NB3-001)
    Kategori NVARCHAR(50) NOT NULL, -- B3, Non-B3, Medis
    NamaLimbah NVARCHAR(100) NOT NULL,
    Deskripsi NVARCHAR(500) NULL,
    PerluIzinKhusus BIT NOT NULL DEFAULT 0,
    CONSTRAINT PK_Dim_JenisLimbah PRIMARY KEY CLUSTERED (JenisLimbahKey),
    CONSTRAINT UQ_Dim_JenisLimbah_KodeLimbah UNIQUE (KodeLimbah)
);
GO

PRINT 'Table Dim_JenisLimbah created successfully.';
GO

PRINT '';
PRINT 'All dimension tables created successfully!';
GO
