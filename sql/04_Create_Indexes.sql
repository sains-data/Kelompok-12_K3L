USE K3L_DataMart;
GO

PRINT '';
PRINT 'Creating Indexes for K3L Data Mart';
PRINT '';

-- SECTION 1: DIMENSION TABLE INDEXES
PRINT 'Creating indexes for Dimension Tables...';
GO

-- Dim_Date Indexes
PRINT 'Creating indexes for Dim_Date...';

-- Unique index on natural key (FullDate)
CREATE UNIQUE NONCLUSTERED INDEX UQ_Dim_Date_FullDate 
    ON dbo.Dim_Date(FullDate);
GO

-- Composite index for common date filtering
CREATE NONCLUSTERED INDEX IX_Dim_Date_Year_Month 
    ON dbo.Dim_Date(Tahun, Bulan);
GO

-- Index for quarter filtering
CREATE NONCLUSTERED INDEX IX_Dim_Date_Year_Quarter 
    ON dbo.Dim_Date(Tahun, Kuartal);
GO

PRINT 'Dim_Date indexes created successfully.';
GO

-- Dim_Lokasi Indexes
PRINT 'Creating indexes for Dim_Lokasi...';

-- Index for filtering by building
CREATE NONCLUSTERED INDEX IX_Dim_Lokasi_NamaGedung 
    ON dbo.Dim_Lokasi(NamaGedung);
GO

-- Index for filtering by status
CREATE NONCLUSTERED INDEX IX_Dim_Lokasi_Status 
    ON dbo.Dim_Lokasi(Status);
GO

-- Composite index for gedung + lantai queries
CREATE NONCLUSTERED INDEX IX_Dim_Lokasi_Gedung_Lantai 
    ON dbo.Dim_Lokasi(NamaGedung, Lantai);
GO

PRINT 'Dim_Lokasi indexes created successfully.';
GO

-- Dim_UnitKerja Indexes
PRINT 'Creating indexes for Dim_UnitKerja...';

-- Composite index for SCD Type 2 lookup (Natural Key + Current Flag)
CREATE NONCLUSTERED INDEX IX_Dim_UnitKerja_KodeUnit_IsCurrent 
    ON dbo.Dim_UnitKerja(KodeUnit, IsCurrent);
GO

-- Index for filtering by category
CREATE NONCLUSTERED INDEX IX_Dim_UnitKerja_Kategori 
    ON dbo.Dim_UnitKerja(Kategori);
GO

PRINT 'Dim_UnitKerja indexes created successfully.';
GO

-- Dim_JenisInsiden Indexes
PRINT 'Creating indexes for Dim_JenisInsiden...';

-- Index for filtering by category 
CREATE NONCLUSTERED INDEX IX_Dim_JenisInsiden_Kategori 
    ON dbo.Dim_JenisInsiden(Kategori);
GO

PRINT 'Dim_JenisInsiden indexes created successfully.';
GO


-- Dim_TingkatKeparahan Indexes

PRINT 'Creating indexes for Dim_TingkatKeparahan...';

-- Index for filtering by level
CREATE NONCLUSTERED INDEX IX_Dim_TingkatKeparahan_Level 
    ON dbo.Dim_TingkatKeparahan(Level);
GO

PRINT 'Dim_TingkatKeparahan indexes created successfully.';
GO


-- Dim_Peralatan Indexes
PRINT 'Creating indexes for Dim_Peralatan...';

-- Index for filtering by equipment type
CREATE NONCLUSTERED INDEX IX_Dim_Peralatan_JenisPeralatan 
    ON dbo.Dim_Peralatan(JenisPeralatan);
GO

-- Index for filtering by status
CREATE NONCLUSTERED INDEX IX_Dim_Peralatan_Status 
    ON dbo.Dim_Peralatan(Status);
GO

-- Index for expiry date alerts and filtering
CREATE NONCLUSTERED INDEX IX_Dim_Peralatan_TglKadaluarsa 
    ON dbo.Dim_Peralatan(TglKadaluarsa)
    WHERE TglKadaluarsa IS NOT NULL;
GO

-- Composite index for equipment type + status queries
CREATE NONCLUSTERED INDEX IX_Dim_Peralatan_Jenis_Status 
    ON dbo.Dim_Peralatan(JenisPeralatan, Status);
GO

PRINT 'Dim_Peralatan indexes created successfully.';
GO


-- Dim_JenisLimbah Indexes
PRINT 'Creating indexes for Dim_JenisLimbah...';

-- Index for filtering by category
CREATE NONCLUSTERED INDEX IX_Dim_JenisLimbah_Kategori 
    ON dbo.Dim_JenisLimbah(Kategori);
GO

-- Index for filtering by permit requirement
CREATE NONCLUSTERED INDEX IX_Dim_JenisLimbah_PerluIzinKhusus 
    ON dbo.Dim_JenisLimbah(PerluIzinKhusus);
GO

PRINT 'Dim_JenisLimbah indexes created successfully.';
GO

PRINT '';
PRINT 'All dimension table indexes created successfully!';
PRINT '';
GO

-- SECTION 2: FACT TABLE INDEXES
PRINT 'Creating indexes for Fact Tables...';
GO

-- Fact_Insiden Indexes
PRINT 'Creating indexes for Fact_Insiden...';

-- Index on DateKey (most common filter for time-series queries)
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_DateKey 
    ON dbo.Fact_Insiden(DateKey);
GO

-- Index on LokasiKey (for location-based analysis)
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_LokasiKey 
    ON dbo.Fact_Insiden(LokasiKey);
GO

-- Index on UnitKerjaKey (for unit-based analysis)
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_UnitKerjaKey 
    ON dbo.Fact_Insiden(UnitKerjaKey);
GO

-- Index on JenisInsidenKey (for incident type analysis)
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_JenisInsidenKey 
    ON dbo.Fact_Insiden(JenisInsidenKey);
GO

-- Index on KeparahanKey (for severity analysis)
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_KeparahanKey 
    ON dbo.Fact_Insiden(KeparahanKey);
GO

-- Index on degenerate dimension (LaporanID) for lookups
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_LaporanID 
    ON dbo.Fact_Insiden(LaporanID);
GO

-- Composite index for common date + location queries
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Date_Lokasi 
    ON dbo.Fact_Insiden(DateKey, LokasiKey);
GO

-- Composite index for date + unit queries
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Date_Unit 
    ON dbo.Fact_Insiden(DateKey, UnitKerjaKey);
GO

PRINT 'Fact_Insiden indexes created successfully.';
GO

-- Fact_Inspeksi Indexes
PRINT 'Creating indexes for Fact_Inspeksi...';

-- Index on DateKey
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_DateKey 
    ON dbo.Fact_Inspeksi(DateKey);
GO

-- Index on LokasiKey
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_LokasiKey 
    ON dbo.Fact_Inspeksi(LokasiKey);
GO

-- Index on PeralatanKey
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_PeralatanKey 
    ON dbo.Fact_Inspeksi(PeralatanKey);
GO

-- Index on degenerate dimension (InspeksiID)
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_InspeksiID 
    ON dbo.Fact_Inspeksi(InspeksiID);
GO

-- Index on StatusKepatuhan for compliance reporting
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_StatusKepatuhan 
    ON dbo.Fact_Inspeksi(StatusKepatuhan);
GO

-- Composite index for date + location queries
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Date_Lokasi 
    ON dbo.Fact_Inspeksi(DateKey, LokasiKey);
GO

-- Composite index for date + compliance queries
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Date_Kepatuhan 
    ON dbo.Fact_Inspeksi(DateKey, StatusKepatuhan);
GO

PRINT 'Fact_Inspeksi indexes created successfully.';
GO

-- Fact_Limbah Indexes
PRINT 'Creating indexes for Fact_Limbah...';

-- Index on DateKey
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_DateKey 
    ON dbo.Fact_Limbah(DateKey);
GO

-- Index on UnitKerjaKey
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_UnitKerjaKey 
    ON dbo.Fact_Limbah(UnitKerjaKey);
GO

-- Index on JenisLimbahKey
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_JenisLimbahKey 
    ON dbo.Fact_Limbah(JenisLimbahKey);
GO

-- Index on degenerate dimension (TransaksiID)
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_TransaksiID 
    ON dbo.Fact_Limbah(TransaksiID);
GO

-- Composite index for date + unit queries
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Date_Unit 
    ON dbo.Fact_Limbah(DateKey, UnitKerjaKey);
GO

-- Composite index for date + waste type queries
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Date_JenisLimbah 
    ON dbo.Fact_Limbah(DateKey, JenisLimbahKey);
GO

PRINT 'Fact_Limbah indexes created successfully.';
GO

PRINT '';
PRINT 'All fact table indexes created successfully!';
PRINT '';
GO

-- SECTION 3: STATISTICS CREATION
PRINT 'Creating statistics for query optimization...';
GO

-- Statistics for Fact_Insiden measures
CREATE STATISTICS STAT_Fact_Insiden_Measures 
    ON dbo.Fact_Insiden(JumlahKerugian, JmlHariHilang, JmlKorban);
GO

-- Statistics for Fact_Inspeksi measures
CREATE STATISTICS STAT_Fact_Inspeksi_Measures 
    ON dbo.Fact_Inspeksi(JmlTemuan, DurasiTemuanTerbuka);
GO

-- Statistics for Fact_Limbah measures
CREATE STATISTICS STAT_Fact_Limbah_Measures 
    ON dbo.Fact_Limbah(VolumeKg, BiayaPengelolaan);
GO

PRINT 'Statistics created successfully.';
GO

-- SECTION 4: VERIFICATION & SUMMARY
PRINT '';
PRINT 'Index Creation Summary';
PRINT '';

-- Count indexes per table
SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    COUNT(*) AS IndexCount,
    SUM(CASE WHEN i.is_unique = 1 THEN 1 ELSE 0 END) AS UniqueIndexes,
    SUM(CASE WHEN i.type_desc = 'CLUSTERED' THEN 1 ELSE 0 END) AS ClusteredIndexes,
    SUM(CASE WHEN i.type_desc = 'NONCLUSTERED' THEN 1 ELSE 0 END) AS NonClusteredIndexes
FROM 
    sys.indexes i
WHERE 
    OBJECT_NAME(i.object_id) IN (
        'Dim_Date', 'Dim_Lokasi', 'Dim_UnitKerja', 'Dim_JenisInsiden', 
        'Dim_TingkatKeparahan', 'Dim_Peralatan', 'Dim_JenisLimbah',
        'Fact_Insiden', 'Fact_Inspeksi', 'Fact_Limbah'
    )
    AND i.name IS NOT NULL
GROUP BY 
    OBJECT_NAME(i.object_id)
ORDER BY 
    TableName;
GO

-- List all created indexes
SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    COL_NAME(ic.object_id, ic.column_id) AS ColumnName
FROM 
    sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE 
    OBJECT_NAME(i.object_id) IN (
        'Dim_Date', 'Dim_Lokasi', 'Dim_UnitKerja', 'Dim_JenisInsiden', 
        'Dim_TingkatKeparahan', 'Dim_Peralatan', 'Dim_JenisLimbah',
        'Fact_Insiden', 'Fact_Inspeksi', 'Fact_Limbah'
    )
    AND i.name IS NOT NULL
    AND i.name NOT LIKE 'PK_%' -- Exclude primary keys
    AND i.name NOT LIKE 'UQ_Dim_%KodeLokasi' -- Exclude existing unique constraints
    AND i.name NOT LIKE 'UQ_Dim_%NoInventaris'
    AND i.name NOT LIKE 'UQ_Dim_%KodeLimbah'
ORDER BY 
    TableName, IndexName, ic.key_ordinal;
GO

PRINT '';
PRINT 'Index creation completed successfully!';
PRINT 'Total indexes created: ~35 indexes';
GO
