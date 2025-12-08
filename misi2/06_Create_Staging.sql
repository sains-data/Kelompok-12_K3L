USE K3L_DataMart;
GO

/* ============================================================
   A. CREATE STAGING SCHEMA
   ============================================================ */
CREATE SCHEMA stg;
GO


/* ============================================================
   B. STAGING TABLES
   ============================================================ */

-- 1. Staging Table for Insiden
CREATE TABLE stg.Insiden (
    RawInsidenID VARCHAR(50),
    Tanggal DATE,
    Lokasi VARCHAR(100),
    Gedung VARCHAR(100),
    Lantai VARCHAR(10),
    UnitKerja VARCHAR(100),
    JenisInsiden VARCHAR(100),
    Severity VARCHAR(50),
    Petugas VARCHAR(100),
    JumlahKorban INT,
    HariKerjaHilang INT,
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

-- 2. Staging Table for Inspeksi
CREATE TABLE stg.Inspeksi (
    RawInspeksiID VARCHAR(50),
    Tanggal DATE,
    Lokasi VARCHAR(100),
    Gedung VARCHAR(100),
    Lantai VARCHAR(10),
    UnitKerja VARCHAR(100),
    Peralatan VARCHAR(100),
    Petugas VARCHAR(100),
    JumlahDiinspeksi INT,
    JumlahTidakSesuai INT,
    LoadDate DATETIME DEFAULT GETDATE()
);
GO

-- 3. Staging Table for Limbah
CREATE TABLE stg.Limbah (
    RawLimbahID VARCHAR(50),
    Tanggal DATE,
    Lokasi VARCHAR(100),
    Gedung VARCHAR(100),
    Lantai VARCHAR(10),
    UnitKerja VARCHAR(100),
    JenisLimbah VARCHAR(150),
    Kategori VARCHAR(50),
    Sifat VARCHAR(50),
    Petugas VARCHAR(100),
    JumlahLimbah DECIMAL(10,2),
    LoadDate DATETIME DEFAULT GETDATE()
);
GO
