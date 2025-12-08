------------------------------------------------------------
-- DESC  : Semua VIEW untuk Dashboard K3L_DataMart
------------------------------------------------------------
USE K3L_DataMart;
GO

------------------------------------------------------------
-- 1. VIEW: Ringkasan Insiden
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_Insiden_Summary') IS NOT NULL 
    DROP VIEW dbo.vw_Insiden_Summary;
GO

CREATE VIEW dbo.vw_Insiden_Summary AS
SELECT 
    d.Tahun,
    uk.NamaUnit,
    l.NamaLokasi,
    ji.NamaInsiden,
    s.TingkatKeparahan,
    COUNT(fi.InsidenID) AS TotalInsiden,
    SUM(fi.JumlahKorban) AS TotalKorban,
    SUM(fi.HariKerjaHilang) AS TotalHariKerjaHilang
FROM Fact_Insiden fi
JOIN Dim_Date d ON fi.DateID = d.DateID
JOIN Dim_Lokasi l ON fi.LokasiID = l.LokasiID
JOIN Dim_UnitKerja uk ON fi.UnitKerjaID = uk.UnitKerjaID
JOIN Dim_JenisInsiden ji ON fi.JenisInsidenID = ji.JenisInsidenID
JOIN Dim_Severity s ON fi.SeverityID = s.SeverityID
GROUP BY 
    d.Tahun, uk.NamaUnit, l.NamaLokasi, ji.NamaInsiden, s.TingkatKeparahan;
GO


------------------------------------------------------------
-- 2. VIEW: Ringkasan Inspeksi
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_Inspeksi_Summary') IS NOT NULL 
    DROP VIEW dbo.vw_Inspeksi_Summary;
GO

CREATE VIEW dbo.vw_Inspeksi_Summary AS
SELECT
    d.Tahun,
    uk.NamaUnit,
    l.NamaLokasi,
    p.NamaPeralatan,
    SUM(fi.JumlahDiinspeksi) AS TotalDiinspeksi,
    SUM(fi.JumlahTidakSesuai) AS TotalTidakSesuai,
    CASE 
        WHEN SUM(fi.JumlahDiinspeksi) = 0 THEN 0
        ELSE (SUM(fi.JumlahTidakSesuai) * 1.0 / SUM(fi.JumlahDiinspeksi)) * 100
    END AS PersentaseKetidaksesuaian
FROM Fact_Inspeksi fi
JOIN Dim_Date d ON fi.DateID = d.DateID
JOIN Dim_Lokasi l ON fi.LokasiID = l.LokasiID
JOIN Dim_UnitKerja uk ON fi.UnitKerjaID = uk.UnitKerjaID
JOIN Dim_JenisPeralatan p ON fi.PeralatanID = p.PeralatanID
GROUP BY d.Tahun, uk.NamaUnit, l.NamaLokasi, p.NamaPeralatan;
GO


------------------------------------------------------------
-- 3. VIEW: Ringkasan Limbah
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_Limbah_Summary') IS NOT NULL 
    DROP VIEW dbo.vw_Limbah_Summary;
GO

CREATE VIEW dbo.vw_Limbah_Summary AS
SELECT
    d.Tahun,
    uk.NamaUnit,
    l.NamaLokasi,
    jl.JenisLimbah,
    jl.Kategori,
    SUM(fl.JumlahLimbah) AS TotalLimbah
FROM Fact_Limbah fl
JOIN Dim_Date d ON fl.DateID = d.DateID
JOIN Dim_Lokasi l ON fl.LokasiID = l.LokasiID
JOIN Dim_UnitKerja uk ON fl.UnitKerjaID = uk.UnitKerjaID
JOIN Dim_JenisLimbah jl ON fl.LimbahID = jl.LimbahID
GROUP BY d.Tahun, uk.NamaUnit, l.NamaLokasi, jl.JenisLimbah, jl.Kategori;
GO


------------------------------------------------------------
-- 4. VIEW: Executive Summary K3L
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_Executive_Summary_K3L') IS NOT NULL 
    DROP VIEW dbo.vw_Executive_Summary_K3L;
GO

CREATE VIEW dbo.vw_Executive_Summary_K3L AS
SELECT 
    d.Tahun,

    -- INSIDEN
    COUNT(DISTINCT fi.InsidenID) AS TotalInsiden,
    SUM(fi.JumlahKorban) AS TotalKorbanInsiden,
    SUM(fi.HariKerjaHilang) AS TotalHariKerjaHilang,

    -- INSPEKSI
    COUNT(DISTINCT insp.InspeksiID) AS TotalInspeksi,
    SUM(insp.JumlahTidakSesuai) AS TotalTemuanKetidaksesuaian,

    -- LIMBAH
    SUM(fl.JumlahLimbah) AS TotalLimbah,

    -- UNIT KERJA
    COUNT(DISTINCT uk.UnitKerjaID) AS TotalUnitKerja
FROM Dim_Date d
LEFT JOIN Fact_Insiden fi ON fi.DateID = d.DateID
LEFT JOIN Fact_Inspeksi insp ON insp.DateID = d.DateID
LEFT JOIN Fact_Limbah fl ON fl.DateID = d.DateID
LEFT JOIN Dim_UnitKerja uk ON uk.UnitKerjaID = fi.UnitKerjaID
GROUP BY d.Tahun;
GO
