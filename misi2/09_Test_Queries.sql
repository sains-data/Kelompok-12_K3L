---------------------------------------------------------------
-- FILE: 09_Performance_Testing.sql
-- Deskripsi: Performance Testing untuk K3L Data Mart
-- Tujuan: Menguji dan mengoptimalkan performa query
---------------------------------------------------------------

USE K3L_DataMart;
GO

-- Aktifkan statistik waktu dan IO untuk analisis performa
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

/* ============================================================
   1. Query 1: Total insiden per lokasi
   ============================================================ */
SELECT
    l.NamaLokasi AS Lokasi,
    COUNT(f.InsidenID) AS TotalInsiden,
    SUM(f.JumlahKorban) AS TotalKorban,
    AVG(f.HariKerjaHilang) AS RataHariHilang
FROM Fact_Insiden f
INNER JOIN Dim_Lokasi l ON f.LokasiID = l.LokasiID
GROUP BY l.NamaLokasi
ORDER BY TotalInsiden DESC;
GO

/* ============================================================
   2. Query 2: Total inspeksi dan ketidaksesuaian per unit kerja
   ============================================================ */
SELECT
    u.NamaUnit AS UnitKerja,
    COUNT(f.InspeksiID) AS TotalInspeksi,
    SUM(f.JumlahTidakSesuai) AS TotalTidakSesuai,
    AVG(f.JumlahDiinspeksi) AS RataDiinspeksi
FROM Fact_Inspeksi f
INNER JOIN Dim_UnitKerja u ON f.UnitKerjaID = u.UnitKerjaID
GROUP BY u.NamaUnit
ORDER BY TotalTidakSesuai DESC;
GO

/* ============================================================
   3. Query 3: Total limbah per jenis dan lokasi
   ============================================================ */
SELECT
    l.NamaLokasi AS Lokasi,
    j.JenisLimbah AS JenisLimbah,
    SUM(f.JumlahLimbah) AS TotalLimbah
FROM Fact_Limbah f
INNER JOIN Dim_Lokasi l ON f.LokasiID = l.LokasiID
INNER JOIN Dim_JenisLimbah j ON f.LimbahID = j.LimbahID
GROUP BY l.NamaLokasi, j.JenisLimbah
ORDER BY TotalLimbah DESC;
GO

/* ============================================================
   4. Query 4: Insiden per severity per bulan
   ============================================================ */
SELECT
    s.TingkatKeparahan AS Severity,
    d.Bulan AS Bulan,
    COUNT(f.InsidenID) AS TotalInsiden
FROM Fact_Insiden f
INNER JOIN Dim_Severity s ON f.SeverityID = s.SeverityID
INNER JOIN Dim_Date d ON f.DateID = d.DateID
GROUP BY s.TingkatKeparahan, d.Bulan
ORDER BY s.TingkatKeparahan, d.Bulan;
GO

/* ============================================================
   5. Query 5: Petugas dengan total aktivitas (insiden + inspeksi + limbah)
   ============================================================ */
SELECT
    p.NamaPetugas AS Petugas,
    SUM(CASE WHEN fi.InsidenID IS NOT NULL THEN 1 ELSE 0 END) AS TotalInsiden,
    SUM(CASE WHEN fip.InspeksiID IS NOT NULL THEN 1 ELSE 0 END) AS TotalInspeksi,
    SUM(CASE WHEN fl.LimbahFactID IS NOT NULL THEN 1 ELSE 0 END) AS TotalLimbah
FROM Dim_Petugas p
LEFT JOIN Fact_Insiden fi ON p.PetugasID = fi.PetugasID
LEFT JOIN Fact_Inspeksi fip ON p.PetugasID = fip.PetugasID
LEFT JOIN Fact_Limbah fl ON p.PetugasID = fl.PetugasID
GROUP BY p.NamaPetugas
ORDER BY TotalInsiden DESC, TotalInspeksi DESC, TotalLimbah DESC;
GO

-- Matikan statistik
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
GO
