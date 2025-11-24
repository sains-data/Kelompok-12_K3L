USE K3L_DataMart;
GO

PRINT 'K3L DATA MART - DATA QUALITY CHECKS';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';
GO


-- SECTION 1: REFERENTIAL INTEGRITY CHECKS
PRINT 'Section 1: Referential Integrity Checks';
PRINT '----------------------------------------';
GO

-- Check 1.1: Orphan records in Fact_Insiden
PRINT 'Check 1.1: Orphan FKs in Fact_Insiden';
SELECT 
    'Fact_Insiden - Invalid DateKey' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Insiden f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Date d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 
    'Fact_Insiden - Invalid LokasiKey',
    COUNT(*)
FROM dbo.Fact_Insiden f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Lokasi d WHERE d.LokasiKey = f.LokasiKey)
UNION ALL
SELECT 
    'Fact_Insiden - Invalid UnitKerjaKey',
    COUNT(*)
FROM dbo.Fact_Insiden f
WHERE f.UnitKerjaKey IS NOT NULL 
    AND NOT EXISTS (SELECT 1 FROM dbo.Dim_UnitKerja d WHERE d.UnitKerjaKey = f.UnitKerjaKey)
UNION ALL
SELECT 
    'Fact_Insiden - Invalid JenisInsidenKey',
    COUNT(*)
FROM dbo.Fact_Insiden f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_JenisInsiden d WHERE d.JenisInsidenKey = f.JenisInsidenKey)
UNION ALL
SELECT 
    'Fact_Insiden - Invalid TingkatKeparahanKey',
    COUNT(*)
FROM dbo.Fact_Insiden f
WHERE f.TingkatKeparahanKey IS NOT NULL 
    AND NOT EXISTS (SELECT 1 FROM dbo.Dim_TingkatKeparahan d WHERE d.TingkatKeparahanKey = f.TingkatKeparahanKey);
GO

-- Check 1.2: Orphan records in Fact_Inspeksi
PRINT 'Check 1.2: Orphan FKs in Fact_Inspeksi';
SELECT 
    'Fact_Inspeksi - Invalid DateKey' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Inspeksi f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Date d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 
    'Fact_Inspeksi - Invalid LokasiKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Lokasi d WHERE d.LokasiKey = f.LokasiKey)
UNION ALL
SELECT 
    'Fact_Inspeksi - Invalid UnitKerjaKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi f
WHERE f.UnitKerjaKey IS NOT NULL 
    AND NOT EXISTS (SELECT 1 FROM dbo.Dim_UnitKerja d WHERE d.UnitKerjaKey = f.UnitKerjaKey)
UNION ALL
SELECT 
    'Fact_Inspeksi - Invalid PeralatanKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Peralatan d WHERE d.PeralatanKey = f.PeralatanKey)
UNION ALL
SELECT 
    'Fact_Inspeksi - Invalid TanggalTindakLanjutKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi f
WHERE f.TanggalTindakLanjutKey IS NOT NULL 
    AND NOT EXISTS (SELECT 1 FROM dbo.Dim_Date d WHERE d.DateKey = f.TanggalTindakLanjutKey);
GO

-- Check 1.3: Orphan records in Fact_Limbah
PRINT 'Check 1.3: Orphan FKs in Fact_Limbah';
SELECT 
    'Fact_Limbah - Invalid DateKey' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Limbah f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Date d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 
    'Fact_Limbah - Invalid LokasiKey',
    COUNT(*)
FROM dbo.Fact_Limbah f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_Lokasi d WHERE d.LokasiKey = f.LokasiKey)
UNION ALL
SELECT 
    'Fact_Limbah - Invalid UnitKerjaKey',
    COUNT(*)
FROM dbo.Fact_Limbah f
WHERE f.UnitKerjaKey IS NOT NULL 
    AND NOT EXISTS (SELECT 1 FROM dbo.Dim_UnitKerja d WHERE d.UnitKerjaKey = f.UnitKerjaKey)
UNION ALL
SELECT 
    'Fact_Limbah - Invalid JenisLimbahKey',
    COUNT(*)
FROM dbo.Fact_Limbah f
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_JenisLimbah d WHERE d.JenisLimbahKey = f.JenisLimbahKey);
GO

PRINT 'Section 1 completed.';
PRINT '';
GO


-- SECTION 2: DATA COMPLETENESS CHECKS
PRINT 'Section 2: Data Completeness Checks';
GO

-- Check 2.1: NULL values in mandatory dimension attributes
PRINT 'Check 2.1: NULL values in Dimension mandatory fields';
SELECT 
    'Dim_Lokasi - NULL NamaLokasi' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Dim_Lokasi
WHERE NamaLokasi IS NULL OR NamaLokasi = ''
UNION ALL
SELECT 
    'Dim_UnitKerja - NULL NamaUnitKerja',
    COUNT(*)
FROM dbo.Dim_UnitKerja
WHERE NamaUnitKerja IS NULL OR NamaUnitKerja = ''
UNION ALL
SELECT 
    'Dim_Peralatan - NULL NamaPeralatan',
    COUNT(*)
FROM dbo.Dim_Peralatan
WHERE NamaPeralatan IS NULL OR NamaPeralatan = ''
UNION ALL
SELECT 
    'Dim_JenisInsiden - NULL NamaJenisInsiden',
    COUNT(*)
FROM dbo.Dim_JenisInsiden
WHERE NamaJenisInsiden IS NULL OR NamaJenisInsiden = ''
UNION ALL
SELECT 
    'Dim_JenisLimbah - NULL NamaJenisLimbah',
    COUNT(*)
FROM dbo.Dim_JenisLimbah
WHERE NamaJenisLimbah IS NULL OR NamaJenisLimbah = '';
GO

-- Check 2.2: NULL values in mandatory fact measures
PRINT 'Check 2.2: NULL values in Fact mandatory fields';
SELECT 
    'Fact_Insiden - NULL DateKey' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Insiden
WHERE DateKey IS NULL
UNION ALL
SELECT 
    'Fact_Insiden - NULL LokasiKey',
    COUNT(*)
FROM dbo.Fact_Insiden
WHERE LokasiKey IS NULL
UNION ALL
SELECT 
    'Fact_Insiden - NULL JenisInsidenKey',
    COUNT(*)
FROM dbo.Fact_Insiden
WHERE JenisInsidenKey IS NULL
UNION ALL
SELECT 
    'Fact_Inspeksi - NULL DateKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi
WHERE DateKey IS NULL
UNION ALL
SELECT 
    'Fact_Inspeksi - NULL LokasiKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi
WHERE LokasiKey IS NULL
UNION ALL
SELECT 
    'Fact_Inspeksi - NULL PeralatanKey',
    COUNT(*)
FROM dbo.Fact_Inspeksi
WHERE PeralatanKey IS NULL
UNION ALL
SELECT 
    'Fact_Limbah - NULL DateKey',
    COUNT(*)
FROM dbo.Fact_Limbah
WHERE DateKey IS NULL
UNION ALL
SELECT 
    'Fact_Limbah - NULL LokasiKey',
    COUNT(*)
FROM dbo.Fact_Limbah
WHERE LokasiKey IS NULL
UNION ALL
SELECT 
    'Fact_Limbah - NULL JenisLimbahKey',
    COUNT(*)
FROM dbo.Fact_Limbah
WHERE JenisLimbahKey IS NULL;
GO

PRINT 'Section 2 completed.';
PRINT '';
GO

-- SECTION 3: DATA VALIDITY CHECKS
PRINT 'Section 3: Data Validity Checks';
GO

-- Check 3.1: Negative values in fact measures
PRINT 'Check 3.1: Negative values in Fact measures';
SELECT 
    'Fact_Insiden - Negative KorbanJiwa' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Insiden
WHERE KorbanJiwa < 0
UNION ALL
SELECT 
    'Fact_Insiden - Negative KorbanLukaRingan',
    COUNT(*)
FROM dbo.Fact_Insiden
WHERE KorbanLukaRingan < 0
UNION ALL
SELECT 
    'Fact_Insiden - Negative KorbanLukaBerat',
    COUNT(*)
FROM dbo.Fact_Insiden
WHERE KorbanLukaBerat < 0
UNION ALL
SELECT 
    'Fact_Insiden - Negative KerugianMateriEstimasi',
    COUNT(*)
FROM dbo.Fact_Insiden
WHERE KerugianMateriEstimasi < 0
UNION ALL
SELECT 
    'Fact_Inspeksi - Negative TemuanMasalah',
    COUNT(*)
FROM dbo.Fact_Inspeksi
WHERE TemuanMasalah < 0
UNION ALL
SELECT 
    'Fact_Limbah - Negative VolumeLimbah',
    COUNT(*)
FROM dbo.Fact_Limbah
WHERE VolumeLimbah < 0
UNION ALL
SELECT 
    'Fact_Limbah - Negative BiayaPengelolaan',
    COUNT(*)
FROM dbo.Fact_Limbah
WHERE BiayaPengelolaan < 0;
GO

-- Check 3.2: Invalid date ranges
PRINT 'Check 3.2: Invalid date ranges in Facts';
SELECT 
    'Fact_Insiden - Future dates' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Insiden f
INNER JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.FullDate > GETDATE()
UNION ALL
SELECT 
    'Fact_Insiden - Dates before ITERA established (2014)',
    COUNT(*)
FROM dbo.Fact_Insiden f
INNER JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.Year < 2014
UNION ALL
SELECT 
    'Fact_Inspeksi - Future dates',
    COUNT(*)
FROM dbo.Fact_Inspeksi f
INNER JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.FullDate > GETDATE()
UNION ALL
SELECT 
    'Fact_Limbah - Future dates',
    COUNT(*)
FROM dbo.Fact_Limbah f
INNER JOIN dbo.Dim_Date d ON f.DateKey = d.DateKey
WHERE d.FullDate > GETDATE();
GO

-- Check 3.3: Invalid WaktuInsiden (should be between 00:00:00 and 23:59:59)
PRINT 'Check 3.3: Invalid WaktuInsiden values';
SELECT 
    'Fact_Insiden - Invalid WaktuInsiden' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.Fact_Insiden
WHERE WaktuInsiden IS NOT NULL 
    AND (WaktuInsiden < '00:00:00' OR WaktuInsiden > '23:59:59');
GO

PRINT 'Section 3 completed.';
PRINT '';
GO


-- SECTION 4: DATA CONSISTENCY CHECKS
PRINT 'Section 4: Data Consistency Checks';
GO

-- Check 4.1: Business rule validation - Total korban vs keparahan
PRINT 'Check 4.1: Korban count vs Tingkat Keparahan consistency';
SELECT 
    f.InsidenKey,
    f.KorbanJiwa,
    f.KorbanLukaRingan,
    f.KorbanLukaBerat,
    tk.NamaTingkatKeparahan,
    'High severity but no casualties' AS Issue
FROM dbo.Fact_Insiden f
LEFT JOIN dbo.Dim_TingkatKeparahan tk ON f.TingkatKeparahanKey = tk.TingkatKeparahanKey
WHERE tk.NamaTingkatKeparahan IN ('Berat', 'Fatal')
    AND (f.KorbanJiwa = 0 OR f.KorbanJiwa IS NULL)
    AND (f.KorbanLukaBerat = 0 OR f.KorbanLukaBerat IS NULL);
GO

-- Check 4.2: Inspeksi with findings but no follow-up
PRINT 'Check 4.2: Inspeksi with TemuanMasalah but no TindakanKoreksi';
SELECT 
    InspeksiKey,
    TemuanMasalah,
    'Has findings but no corrective action' AS Issue
FROM dbo.Fact_Inspeksi
WHERE TemuanMasalah > 0 
    AND (TindakanKoreksi IS NULL OR TindakanKoreksi = '');
GO

-- Check 4.3: Limbah without proper handling method
PRINT 'Check 4.3: Limbah without MetodePengelolaan';
SELECT 
    LimbahKey,
    VolumeLimbah,
    'Volume recorded but no handling method' AS Issue
FROM dbo.Fact_Limbah
WHERE VolumeLimbah > 0 
    AND (MetodePengelolaan IS NULL OR MetodePengelolaan = '');
GO

-- Check 4.4: SCD Type 2 validation for Dim_UnitKerja
PRINT 'Check 4.4: SCD Type 2 overlapping dates in Dim_UnitKerja';
SELECT 
    u1.NamaUnitKerja,
    u1.StartDate AS StartDate1,
    u1.EndDate AS EndDate1,
    u2.StartDate AS StartDate2,
    u2.EndDate AS EndDate2,
    'Overlapping SCD Type 2 records' AS Issue
FROM dbo.Dim_UnitKerja u1
INNER JOIN dbo.Dim_UnitKerja u2 ON u1.NamaUnitKerja = u2.NamaUnitKerja
    AND u1.UnitKerjaKey <> u2.UnitKerjaKey
WHERE u1.StartDate < u2.EndDate 
    AND u2.StartDate < u1.EndDate;
GO

PRINT 'Section 4 completed.';
PRINT '';
GO

-- SECTION 5: DUPLICATE DETECTION
PRINT 'Section 5: Duplicate Detection';
GO

-- Check 5.1: Duplicate dimension records (degenerate dimensions)
PRINT 'Check 5.1: Duplicate natural keys in Dimensions';
SELECT 
    'Dim_Lokasi - Duplicate NamaLokasi' AS Issue,
    NamaLokasi,
    COUNT(*) AS DuplicateCount
FROM dbo.Dim_Lokasi
GROUP BY NamaLokasi
HAVING COUNT(*) > 1
UNION ALL
SELECT 
    'Dim_Peralatan - Duplicate KodePeralatan',
    KodePeralatan,
    COUNT(*)
FROM dbo.Dim_Peralatan
GROUP BY KodePeralatan
HAVING COUNT(*) > 1
UNION ALL
SELECT 
    'Dim_JenisInsiden - Duplicate NamaJenisInsiden',
    NamaJenisInsiden,
    COUNT(*)
FROM dbo.Dim_JenisInsiden
GROUP BY NamaJenisInsiden
HAVING COUNT(*) > 1
UNION ALL
SELECT 
    'Dim_TingkatKeparahan - Duplicate NamaTingkatKeparahan',
    NamaTingkatKeparahan,
    COUNT(*)
FROM dbo.Dim_TingkatKeparahan
GROUP BY NamaTingkatKeparahan
HAVING COUNT(*) > 1
UNION ALL
SELECT 
    'Dim_JenisLimbah - Duplicate NamaJenisLimbah',
    NamaJenisLimbah,
    COUNT(*)
FROM dbo.Dim_JenisLimbah
GROUP BY NamaJenisLimbah
HAVING COUNT(*) > 1;
GO

-- Check 5.2: Duplicate fact records (same business key)
PRINT 'Check 5.2: Duplicate business keys in Fact tables';
-- Note: Fact tables should allow duplicates by design (multiple events same day/location)
-- This check is for informational purposes only
SELECT 
    'Fact_Insiden - Same Date/Lokasi/JenisInsiden' AS Info,
    COUNT(*) AS RecordCount
FROM (
    SELECT DateKey, LokasiKey, JenisInsidenKey, COUNT(*) AS cnt
    FROM dbo.Fact_Insiden
    GROUP BY DateKey, LokasiKey, JenisInsidenKey
    HAVING COUNT(*) > 1
) duplicates;
GO

PRINT 'Section 5 completed.';
PRINT '';
GO

-- SECTION 6: STAGING DATA QUALITY
PRINT 'Section 6: Staging Data Quality';
PRINT '----------------------------------------';
GO

-- Check 6.1: Unprocessed records in staging
PRINT 'Check 6.1: Unprocessed records in Staging tables';
SELECT 
    'STG_Lokasi - Unprocessed' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.STG_Lokasi
WHERE IsProcessed = 0
UNION ALL
SELECT 
    'STG_UnitKerja - Unprocessed',
    COUNT(*)
FROM dbo.STG_UnitKerja
WHERE IsProcessed = 0
UNION ALL
SELECT 
    'STG_Peralatan - Unprocessed',
    COUNT(*)
FROM dbo.STG_Peralatan
WHERE IsProcessed = 0
UNION ALL
SELECT 
    'STG_Insiden - Unprocessed',
    COUNT(*)
FROM dbo.STG_Insiden
WHERE IsProcessed = 0
UNION ALL
SELECT 
    'STG_Inspeksi - Unprocessed',
    COUNT(*)
FROM dbo.STG_Inspeksi
WHERE IsProcessed = 0
UNION ALL
SELECT 
    'STG_Limbah - Unprocessed',
    COUNT(*)
FROM dbo.STG_Limbah
WHERE IsProcessed = 0;
GO

-- Check 6.2: Records with ETL errors
PRINT 'Check 6.2: Records with ETL errors in Staging';
SELECT 
    'STG_Lokasi - With Errors' AS Issue,
    COUNT(*) AS RecordCount
FROM dbo.STG_Lokasi
WHERE ETL_ErrorMessage IS NOT NULL
UNION ALL
SELECT 
    'STG_UnitKerja - With Errors',
    COUNT(*)
FROM dbo.STG_UnitKerja
WHERE ETL_ErrorMessage IS NOT NULL
UNION ALL
SELECT 
    'STG_Peralatan - With Errors',
    COUNT(*)
FROM dbo.STG_Peralatan
WHERE ETL_ErrorMessage IS NOT NULL
UNION ALL
SELECT 
    'STG_Insiden - With Errors',
    COUNT(*)
FROM dbo.STG_Insiden
WHERE ETL_ErrorMessage IS NOT NULL
UNION ALL
SELECT 
    'STG_Inspeksi - With Errors',
    COUNT(*)
FROM dbo.STG_Inspeksi
WHERE ETL_ErrorMessage IS NOT NULL
UNION ALL
SELECT 
    'STG_Limbah - With Errors',
    COUNT(*)
FROM dbo.STG_Limbah
WHERE ETL_ErrorMessage IS NOT NULL;
GO

-- Check 6.3: Detailed error messages
PRINT 'Check 6.3: Sample ETL error messages from Staging';
SELECT TOP 20
    'STG_Insiden' AS TableName,
    ETL_ErrorMessage,
    ETL_InsertDate
FROM dbo.STG_Insiden
WHERE ETL_ErrorMessage IS NOT NULL
ORDER BY ETL_InsertDate DESC;
GO

PRINT 'Section 6 completed.';
PRINT '';
GO

-- SECTION 7: SUMMARY STATISTICS
PRINT 'Section 7: Data Quality Summary';
GO

-- Summary of record counts
PRINT 'Record Count Summary:';
SELECT 
    'Dim_Date' AS TableName,
    COUNT(*) AS RecordCount,
    'Dimension' AS TableType
FROM dbo.Dim_Date
UNION ALL
SELECT 'Dim_Lokasi', COUNT(*), 'Dimension' FROM dbo.Dim_Lokasi
UNION ALL
SELECT 'Dim_UnitKerja', COUNT(*), 'Dimension' FROM dbo.Dim_UnitKerja
UNION ALL
SELECT 'Dim_JenisInsiden', COUNT(*), 'Dimension' FROM dbo.Dim_JenisInsiden
UNION ALL
SELECT 'Dim_TingkatKeparahan', COUNT(*), 'Dimension' FROM dbo.Dim_TingkatKeparahan
UNION ALL
SELECT 'Dim_Peralatan', COUNT(*), 'Dimension' FROM dbo.Dim_Peralatan
UNION ALL
SELECT 'Dim_JenisLimbah', COUNT(*), 'Dimension' FROM dbo.Dim_JenisLimbah
UNION ALL
SELECT 'Fact_Insiden', COUNT(*), 'Fact' FROM dbo.Fact_Insiden
UNION ALL
SELECT 'Fact_Inspeksi', COUNT(*), 'Fact' FROM dbo.Fact_Inspeksi
UNION ALL
SELECT 'Fact_Limbah', COUNT(*), 'Fact' FROM dbo.Fact_Limbah
ORDER BY TableType, TableName;
GO

PRINT '';
PRINT 'DATA QUALITY CHECKS COMPLETED';
GO
