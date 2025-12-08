USE K3L_DataMart;
GO

/* ============================================================
   1. Dim_Date
   ============================================================ */
IF OBJECT_ID('dbo.Dim_Date', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_Date;
GO

CREATE TABLE dbo.Dim_Date
(
    DateKey INT NOT NULL PRIMARY KEY,
    Tanggal DATE NOT NULL,
    Bulan INT NOT NULL,
    Kuartal INT NOT NULL,
    Tahun INT NOT NULL
);
GO


/* ============================================================
   2. Dim_Lokasi
   ============================================================ */
IF OBJECT_ID('dbo.Dim_Lokasi', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_Lokasi;
GO

CREATE TABLE dbo.Dim_Lokasi
(
    LokasiKey INT IDENTITY(1,1) PRIMARY KEY,
    Gedung NVARCHAR(100) NOT NULL,
    Lantai NVARCHAR(20) NOT NULL,
    Ruangan NVARCHAR(100) NOT NULL
);
GO


/* ============================================================
   3. Dim_JenisInsiden
   ============================================================ */
IF OBJECT_ID('dbo.Dim_JenisInsiden', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_JenisInsiden;
GO

CREATE TABLE dbo.Dim_JenisInsiden
(
    JenisInsidenKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaJenisInsiden NVARCHAR(100) NOT NULL
);
GO


/* ============================================================
   4. Dim_TingkatKeparahan
   ============================================================ */
IF OBJECT_ID('dbo.Dim_TingkatKeparahan', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_TingkatKeparahan;
GO

CREATE TABLE dbo.Dim_TingkatKeparahan
(
    TingkatKeparahanKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaTingkatKeparahan NVARCHAR(50) NOT NULL
);
GO


/* ============================================================
   5. Dim_UnitKerja
   ============================================================ */
IF OBJECT_ID('dbo.Dim_UnitKerja', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_UnitKerja;
GO

CREATE TABLE dbo.Dim_UnitKerja
(
    UnitKerjaKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaUnitKerja NVARCHAR(100) NOT NULL
);
GO


/* ============================================================
   6. Dim_JenisPeralatan
   ============================================================ */
IF OBJECT_ID('dbo.Dim_JenisPeralatan', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_JenisPeralatan;
GO

CREATE TABLE dbo.Dim_JenisPeralatan
(
    JenisPeralatanKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaJenisPeralatan NVARCHAR(100) NOT NULL
);
GO


/* ============================================================
   7. Dim_JenisLimbah
   ============================================================ */
IF OBJECT_ID('dbo.Dim_JenisLimbah', 'U') IS NOT NULL
    DROP TABLE dbo.Dim_JenisLimbah;
GO

CREATE TABLE dbo.Dim_JenisLimbah
(
    JenisLimbahKey INT IDENTITY(1,1) PRIMARY KEY,
    NamaJenisLimbah NVARCHAR(100) NOT NULL
);
GO

