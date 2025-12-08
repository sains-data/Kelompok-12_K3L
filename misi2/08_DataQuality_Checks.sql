USE K3L_DataMart;
GO

/* ============================================================
   1. Fact_Insiden Summary
   ============================================================ */
SELECT
    'Fact_Insiden' AS TableName,
    COUNT(*) AS TotalRecord,
    SUM(CASE WHEN DateID IS NULL THEN 1 ELSE 0 END) AS NullDateID,
    SUM(CASE WHEN LokasiID IS NULL THEN 1 ELSE 0 END) AS NullLokasiID,
    SUM(CASE WHEN UnitKerjaID IS NULL THEN 1 ELSE 0 END) AS NullUnitKerjaID,
    SUM(CASE WHEN JenisInsidenID IS NULL THEN 1 ELSE 0 END) AS NullJenisInsidenID,
    SUM(CASE WHEN SeverityID IS NULL THEN 1 ELSE 0 END) AS NullSeverityID,
    SUM(CASE WHEN PetugasID IS NULL THEN 1 ELSE 0 END) AS NullPetugasID
FROM Fact_Insiden;
GO

-- Orphan check per FK
SELECT
    'Fact_Insiden_DateID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_Date d ON f.DateID = d.DateID
WHERE d.DateID IS NULL;

SELECT
    'Fact_Insiden_LokasiID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_Lokasi l ON f.LokasiID = l.LokasiID
WHERE l.LokasiID IS NULL;

SELECT
    'Fact_Insiden_UnitKerjaID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_UnitKerja u ON f.UnitKerjaID = u.UnitKerjaID
WHERE u.UnitKerjaID IS NULL;

SELECT
    'Fact_Insiden_JenisInsidenID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_JenisInsiden j ON f.JenisInsidenID = j.JenisInsidenID
WHERE j.JenisInsidenID IS NULL;

SELECT
    'Fact_Insiden_SeverityID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_Severity s ON f.SeverityID = s.SeverityID
WHERE s.SeverityID IS NULL;

SELECT
    'Fact_Insiden_PetugasID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Insiden f
LEFT JOIN Dim_Petugas p ON f.PetugasID = p.PetugasID
WHERE p.PetugasID IS NULL;

-- Duplicate check
SELECT 'Fact_Insiden' AS TableName,
       DateID, LokasiID, UnitKerjaID, JenisInsidenID, SeverityID, PetugasID,
       COUNT(*) AS DupCount
FROM Fact_Insiden
GROUP BY DateID, LokasiID, UnitKerjaID, JenisInsidenID, SeverityID, PetugasID
HAVING COUNT(*) > 1;
GO


/* ============================================================
   2. Fact_Inspeksi Summary
   ============================================================ */
SELECT
    'Fact_Inspeksi' AS TableName,
    COUNT(*) AS TotalRecord,
    SUM(CASE WHEN DateID IS NULL THEN 1 ELSE 0 END) AS NullDateID,
    SUM(CASE WHEN LokasiID IS NULL THEN 1 ELSE 0 END) AS NullLokasiID,
    SUM(CASE WHEN UnitKerjaID IS NULL THEN 1 ELSE 0 END) AS NullUnitKerjaID,
    SUM(CASE WHEN PeralatanID IS NULL THEN 1 ELSE 0 END) AS NullPeralatanID,
    SUM(CASE WHEN PetugasID IS NULL THEN 1 ELSE 0 END) AS NullPetugasID
FROM Fact_Inspeksi;
GO

-- Orphan check
SELECT 'Fact_Inspeksi_DateID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Inspeksi f
LEFT JOIN Dim_Date d ON f.DateID = d.DateID
WHERE d.DateID IS NULL;

SELECT 'Fact_Inspeksi_LokasiID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Inspeksi f
LEFT JOIN Dim_Lokasi l ON f.LokasiID = l.LokasiID
WHERE l.LokasiID IS NULL;

SELECT 'Fact_Inspeksi_UnitKerjaID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Inspeksi f
LEFT JOIN Dim_UnitKerja u ON f.UnitKerjaID = u.UnitKerjaID
WHERE u.UnitKerjaID IS NULL;

SELECT 'Fact_Inspeksi_PeralatanID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Inspeksi f
LEFT JOIN Dim_JenisPeralatan p ON f.PeralatanID = p.PeralatanID
WHERE p.PeralatanID IS NULL;

SELECT 'Fact_Inspeksi_PetugasID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Inspeksi f
LEFT JOIN Dim_Petugas pt ON f.PetugasID = pt.PetugasID
WHERE pt.PetugasID IS NULL;

-- Duplicate check
SELECT 'Fact_Inspeksi' AS TableName,
       DateID, LokasiID, UnitKerjaID, PeralatanID, PetugasID,
       COUNT(*) AS DupCount
FROM Fact_Inspeksi
GROUP BY DateID, LokasiID, UnitKerjaID, PeralatanID, PetugasID
HAVING COUNT(*) > 1;
GO


/* ============================================================
   3. Fact_Limbah Summary
   ============================================================ */
SELECT
    'Fact_Limbah' AS TableName,
    COUNT(*) AS TotalRecord,
    SUM(CASE WHEN DateID IS NULL THEN 1 ELSE 0 END) AS NullDateID,
    SUM(CASE WHEN LokasiID IS NULL THEN 1 ELSE 0 END) AS NullLokasiID,
    SUM(CASE WHEN UnitKerjaID IS NULL THEN 1 ELSE 0 END) AS NullUnitKerjaID,
    SUM(CASE WHEN LimbahID IS NULL THEN 1 ELSE 0 END) AS NullLimbahID,
    SUM(CASE WHEN PetugasID IS NULL THEN 1 ELSE 0 END) AS NullPetugasID
FROM Fact_Limbah;
GO

-- Orphan check
SELECT 'Fact_Limbah_DateID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Limbah f
LEFT JOIN Dim_Date d ON f.DateID = d.DateID
WHERE d.DateID IS NULL;

SELECT 'Fact_Limbah_LokasiID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Limbah f
LEFT JOIN Dim_Lokasi l ON f.LokasiID = l.LokasiID
WHERE l.LokasiID IS NULL;

SELECT 'Fact_Limbah_UnitKerjaID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Limbah f
LEFT JOIN Dim_UnitKerja u ON f.UnitKerjaID = u.UnitKerjaID
WHERE u.UnitKerjaID IS NULL;

SELECT 'Fact_Limbah_LimbahID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Limbah f
LEFT JOIN Dim_JenisLimbah l ON f.LimbahID = l.LimbahID
WHERE l.LimbahID IS NULL;

SELECT 'Fact_Limbah_PetugasID' AS CheckName, COUNT(*) AS OrphanCount
FROM Fact_Limbah f
LEFT JOIN Dim_Petugas pt ON f.PetugasID = pt.PetugasID
WHERE pt.PetugasID IS NULL;

-- Duplicate check
SELECT 'Fact_Limbah' AS TableName,
       DateID, LokasiID, UnitKerjaID, LimbahID, PetugasID,
       COUNT(*) AS DupCount
FROM Fact_Limbah
GROUP BY DateID, LokasiID, UnitKerjaID, LimbahID, PetugasID
HAVING COUNT(*) > 1;
GO
