USE K3L_DataMart;
GO

PRINT '';
PRINT 'Creating ETL Stored Procedures';
PRINT '';

-- SECTION 1: HELPER PROCEDURES - SURROGATE KEY LOOKUPS
PRINT 'Section 1: Creating Helper Procedures for Surrogate Key Lookups...';
GO

-- Helper: Get LokasiKey by KodeLokasi
IF OBJECT_ID('dbo.usp_GetSurrogateKey_Lokasi', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_Lokasi;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_Lokasi
    @KodeLokasi NVARCHAR(20),
    @LokasiKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @LokasiKey = LokasiKey
    FROM dbo.Dim_Lokasi
    WHERE KodeLokasi = @KodeLokasi;
    
    -- Return -1 if not found (unknown dimension)
    IF @LokasiKey IS NULL
        SET @LokasiKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_Lokasi created.';
GO

-- Helper: Get UnitKerjaKey by KodeUnit (SCD Type 2 - get current)
IF OBJECT_ID('dbo.usp_GetSurrogateKey_UnitKerja', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_UnitKerja;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_UnitKerja
    @KodeUnit NVARCHAR(20),
    @UnitKerjaKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Get current version (IsCurrent = 1) for SCD Type 2
    SELECT @UnitKerjaKey = UnitKerjaKey
    FROM dbo.Dim_UnitKerja
    WHERE KodeUnit = @KodeUnit
      AND IsCurrent = 1;
    
    -- Return -1 if not found
    IF @UnitKerjaKey IS NULL
        SET @UnitKerjaKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_UnitKerja created.';
GO


-- Helper: Get PeralatanKey by NoInventaris
IF OBJECT_ID('dbo.usp_GetSurrogateKey_Peralatan', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_Peralatan;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_Peralatan
    @NoInventaris NVARCHAR(50),
    @PeralatanKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @PeralatanKey = PeralatanKey
    FROM dbo.Dim_Peralatan
    WHERE NoInventaris = @NoInventaris;
    
    -- Return -1 if not found
    IF @PeralatanKey IS NULL
        SET @PeralatanKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_Peralatan created.';
GO


-- Helper: Get JenisInsidenKey by NamaJenisInsiden
IF OBJECT_ID('dbo.usp_GetSurrogateKey_JenisInsiden', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_JenisInsiden;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_JenisInsiden
    @NamaJenisInsiden NVARCHAR(100),
    @JenisInsidenKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @JenisInsidenKey = JenisInsidenKey
    FROM dbo.Dim_JenisInsiden
    WHERE NamaJenisInsiden = @NamaJenisInsiden;
    
    -- Return -1 if not found
    IF @JenisInsidenKey IS NULL
        SET @JenisInsidenKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_JenisInsiden created.';
GO


-- Helper: Get KeparahanKey by NamaTingkatKeparahan
IF OBJECT_ID('dbo.usp_GetSurrogateKey_TingkatKeparahan', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_TingkatKeparahan;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_TingkatKeparahan
    @NamaTingkatKeparahan NVARCHAR(50),
    @KeparahanKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @KeparahanKey = KeparahanKey
    FROM dbo.Dim_TingkatKeparahan
    WHERE NamaTingkatKeparahan = @NamaTingkatKeparahan;
    
    -- Return -1 if not found
    IF @KeparahanKey IS NULL
        SET @KeparahanKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_TingkatKeparahan created.';
GO


-- Helper: Get JenisLimbahKey by KodeLimbah
IF OBJECT_ID('dbo.usp_GetSurrogateKey_JenisLimbah', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetSurrogateKey_JenisLimbah;
GO

CREATE PROCEDURE dbo.usp_GetSurrogateKey_JenisLimbah
    @KodeLimbah NVARCHAR(20),
    @JenisLimbahKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT @JenisLimbahKey = JenisLimbahKey
    FROM dbo.Dim_JenisLimbah
    WHERE KodeLimbah = @KodeLimbah;
    
    -- Return -1 if not found
    IF @JenisLimbahKey IS NULL
        SET @JenisLimbahKey = -1;
END
GO

PRINT 'usp_GetSurrogateKey_JenisLimbah created.';
GO

PRINT 'All helper procedures created successfully.';
PRINT '';
GO

-- SECTION 2: DIMENSION LOAD PROCEDURES
PRINT 'Section 2: Creating Dimension Load Procedures...';
GO

-- Dimension Load: Dim_Lokasi (SCD Type 1 - Overwrite)
IF OBJECT_ID('dbo.usp_Load_Dim_Lokasi', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Dim_Lokasi;
GO

CREATE PROCEDURE dbo.usp_Load_Dim_Lokasi
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    BEGIN TRY
        PRINT 'Loading Dim_Lokasi from staging...';
        
        -- Process unprocessed records
        DECLARE @KodeLokasi NVARCHAR(20);
        DECLARE @NamaGedung NVARCHAR(100);
        DECLARE @Lantai NVARCHAR(20);
        DECLARE @NamaRuangan NVARCHAR(100);
        DECLARE @Kapasitas INT;
        DECLARE @LuasM2 DECIMAL(10,2);
        DECLARE @Status NVARCHAR(20);
        DECLARE @STG_ID INT;
        DECLARE @ExistingKey INT;
        
        DECLARE staging_cursor CURSOR FOR
        SELECT STG_ID, KodeLokasi, NamaGedung, Lantai, NamaRuangan, 
               Kapasitas, LuasM2, Status
        FROM dbo.STG_Lokasi
        WHERE ETL_ProcessedFlag = 0;
        
        OPEN staging_cursor;
        
        FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeLokasi, @NamaGedung, 
              @Lantai, @NamaRuangan, @Kapasitas, @LuasM2, @Status;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validate required fields
                IF @KodeLokasi IS NULL OR LTRIM(RTRIM(@KodeLokasi)) = ''
                BEGIN
                    UPDATE dbo.STG_Lokasi
                    SET ETL_ErrorMessage = 'KodeLokasi is required'
                    WHERE STG_ID = @STG_ID;
                    
                    SET @ErrorCount = @ErrorCount + 1;
                    FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeLokasi, @NamaGedung, 
                          @Lantai, @NamaRuangan, @Kapasitas, @LuasM2, @Status;
                    CONTINUE;
                END
                
                -- Check if record exists
                SELECT @ExistingKey = LokasiKey
                FROM dbo.Dim_Lokasi
                WHERE KodeLokasi = @KodeLokasi;
                
                IF @ExistingKey IS NOT NULL
                BEGIN
                    -- UPDATE (SCD Type 1)
                    UPDATE dbo.Dim_Lokasi
                    SET NamaGedung = ISNULL(@NamaGedung, NamaGedung),
                        Lantai = ISNULL(@Lantai, Lantai),
                        NamaRuangan = ISNULL(@NamaRuangan, NamaRuangan),
                        Kapasitas = @Kapasitas,
                        LuasM2 = @LuasM2,
                        Status = ISNULL(@Status, 'Aktif')
                    WHERE LokasiKey = @ExistingKey;
                    
                    SET @UpdatedCount = @UpdatedCount + 1;
                END
                ELSE
                BEGIN
                    -- INSERT new record
                    INSERT INTO dbo.Dim_Lokasi (KodeLokasi, NamaGedung, Lantai, NamaRuangan, 
                                                Kapasitas, LuasM2, Status)
                    VALUES (@KodeLokasi, @NamaGedung, @Lantai, @NamaRuangan,
                            @Kapasitas, @LuasM2, ISNULL(@Status, 'Aktif'));
                    
                    SET @InsertedCount = @InsertedCount + 1;
                END
                
                -- Mark as processed
                UPDATE dbo.STG_Lokasi
                SET ETL_ProcessedFlag = 1
                WHERE STG_ID = @STG_ID;
                
            END TRY
            BEGIN CATCH
                -- Log error
                UPDATE dbo.STG_Lokasi
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE STG_ID = @STG_ID;
                
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeLokasi, @NamaGedung, 
                  @Lantai, @NamaRuangan, @Kapasitas, @LuasM2, @Status;
        END
        
        CLOSE staging_cursor;
        DEALLOCATE staging_cursor;
        
        PRINT 'Dim_Lokasi load completed.';
        PRINT 'Inserted: ' + CAST(@InsertedCount AS VARCHAR(10));
        PRINT 'Updated: ' + CAST(@UpdatedCount AS VARCHAR(10));
        PRINT 'Errors: ' + CAST(@ErrorCount AS VARCHAR(10));
        
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('global', 'staging_cursor') >= 0
        BEGIN
            CLOSE staging_cursor;
            DEALLOCATE staging_cursor;
        END
        
        PRINT 'Error in Dim_Lokasi load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Dim_Lokasi created.';
GO


-- Dimension Load: Dim_UnitKerja (SCD Type 2 - Track History)
IF OBJECT_ID('dbo.usp_Load_Dim_UnitKerja', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Dim_UnitKerja;
GO

CREATE PROCEDURE dbo.usp_Load_Dim_UnitKerja
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    BEGIN TRY
        PRINT 'Loading Dim_UnitKerja from staging (SCD Type 2)...';
        
        DECLARE @KodeUnit NVARCHAR(20);
        DECLARE @NamaUnit NVARCHAR(100);
        DECLARE @Kategori NVARCHAR(50);
        DECLARE @STG_ID INT;
        DECLARE @ExistingKey INT;
        DECLARE @ExistingNama NVARCHAR(100);
        DECLARE @ExistingKategori NVARCHAR(50);
        
        DECLARE staging_cursor CURSOR FOR
        SELECT STG_ID, KodeUnit, NamaUnit, Kategori
        FROM dbo.STG_UnitKerja
        WHERE ETL_ProcessedFlag = 0;
        
        OPEN staging_cursor;
        
        FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeUnit, @NamaUnit, @Kategori;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validate required fields
                IF @KodeUnit IS NULL OR LTRIM(RTRIM(@KodeUnit)) = ''
                BEGIN
                    UPDATE dbo.STG_UnitKerja
                    SET ETL_ErrorMessage = 'KodeUnit is required'
                    WHERE STG_ID = @STG_ID;
                    
                    SET @ErrorCount = @ErrorCount + 1;
                    FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeUnit, @NamaUnit, @Kategori;
                    CONTINUE;
                END
                
                -- Check if current version exists
                SELECT @ExistingKey = UnitKerjaKey,
                       @ExistingNama = NamaUnit,
                       @ExistingKategori = Kategori
                FROM dbo.Dim_UnitKerja
                WHERE KodeUnit = @KodeUnit
                  AND IsCurrent = 1;
                
                IF @ExistingKey IS NOT NULL
                BEGIN
                    -- Check if data has changed
                    IF @ExistingNama <> @NamaUnit OR @ExistingKategori <> @Kategori
                    BEGIN
                        -- Expire old record
                        UPDATE dbo.Dim_UnitKerja
                        SET IsCurrent = 0,
                            ExpiryDate = CAST(GETDATE() AS DATE)
                        WHERE UnitKerjaKey = @ExistingKey;
                        
                        -- Insert new version
                        INSERT INTO dbo.Dim_UnitKerja (KodeUnit, NamaUnit, Kategori, 
                                                       EffectiveDate, IsCurrent)
                        VALUES (@KodeUnit, @NamaUnit, @Kategori, 
                                CAST(GETDATE() AS DATE), 1);
                        
                        SET @InsertedCount = @InsertedCount + 1;
                        SET @UpdatedCount = @UpdatedCount + 1;
                    END
                    -- else: No change, do nothing
                END
                ELSE
                BEGIN
                    -- Insert new record
                    INSERT INTO dbo.Dim_UnitKerja (KodeUnit, NamaUnit, Kategori, 
                                                   EffectiveDate, IsCurrent)
                    VALUES (@KodeUnit, @NamaUnit, @Kategori, 
                            CAST(GETDATE() AS DATE), 1);
                    
                    SET @InsertedCount = @InsertedCount + 1;
                END
                
                -- Mark as processed
                UPDATE dbo.STG_UnitKerja
                SET ETL_ProcessedFlag = 1
                WHERE STG_ID = @STG_ID;
                
            END TRY
            BEGIN CATCH
                UPDATE dbo.STG_UnitKerja
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE STG_ID = @STG_ID;
                
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            FETCH NEXT FROM staging_cursor INTO @STG_ID, @KodeUnit, @NamaUnit, @Kategori;
        END
        
        CLOSE staging_cursor;
        DEALLOCATE staging_cursor;
        
        PRINT 'Dim_UnitKerja load completed.';
        PRINT 'New records: ' + CAST(@InsertedCount AS VARCHAR(10));
        PRINT 'Updated (expired + new): ' + CAST(@UpdatedCount AS VARCHAR(10));
        PRINT 'Errors: ' + CAST(@ErrorCount AS VARCHAR(10));
        
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('global', 'staging_cursor') >= 0
        BEGIN
            CLOSE staging_cursor;
            DEALLOCATE staging_cursor;
        END
        
        PRINT 'Error in Dim_UnitKerja load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Dim_UnitKerja created.';
GO


-- Dimension Load: Dim_Peralatan (SCD Type 1)
IF OBJECT_ID('dbo.usp_Load_Dim_Peralatan', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Dim_Peralatan;
GO

CREATE PROCEDURE dbo.usp_Load_Dim_Peralatan
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    BEGIN TRY
        PRINT 'Loading Dim_Peralatan from staging...';
        
        DECLARE @NoInventaris NVARCHAR(50);
        DECLARE @JenisPeralatan NVARCHAR(100);
        DECLARE @Merek NVARCHAR(100);
        DECLARE @Model NVARCHAR(100);
        DECLARE @TglPemasangan DATE;
        DECLARE @TglKadaluarsa DATE;
        DECLARE @Status NVARCHAR(20);
        DECLARE @STG_ID INT;
        DECLARE @ExistingKey INT;
        
        DECLARE staging_cursor CURSOR FOR
        SELECT STG_ID, NoInventaris, JenisPeralatan, Merek, Model,
               TglPemasangan, TglKadaluarsa, Status
        FROM dbo.STG_Peralatan
        WHERE ETL_ProcessedFlag = 0;
        
        OPEN staging_cursor;
        
        FETCH NEXT FROM staging_cursor INTO @STG_ID, @NoInventaris, @JenisPeralatan,
              @Merek, @Model, @TglPemasangan, @TglKadaluarsa, @Status;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validate
                IF @NoInventaris IS NULL OR LTRIM(RTRIM(@NoInventaris)) = ''
                BEGIN
                    UPDATE dbo.STG_Peralatan
                    SET ETL_ErrorMessage = 'NoInventaris is required'
                    WHERE STG_ID = @STG_ID;
                    
                    SET @ErrorCount = @ErrorCount + 1;
                    FETCH NEXT FROM staging_cursor INTO @STG_ID, @NoInventaris, @JenisPeralatan,
                          @Merek, @Model, @TglPemasangan, @TglKadaluarsa, @Status;
                    CONTINUE;
                END
                
                -- Check if exists
                SELECT @ExistingKey = PeralatanKey
                FROM dbo.Dim_Peralatan
                WHERE NoInventaris = @NoInventaris;
                
                IF @ExistingKey IS NOT NULL
                BEGIN
                    -- UPDATE
                    UPDATE dbo.Dim_Peralatan
                    SET JenisPeralatan = ISNULL(@JenisPeralatan, JenisPeralatan),
                        Merek = @Merek,
                        Model = @Model,
                        TglPemasangan = @TglPemasangan,
                        TglKadaluarsa = @TglKadaluarsa,
                        Status = ISNULL(@Status, 'Aktif')
                    WHERE PeralatanKey = @ExistingKey;
                    
                    SET @UpdatedCount = @UpdatedCount + 1;
                END
                ELSE
                BEGIN
                    -- INSERT
                    INSERT INTO dbo.Dim_Peralatan (NoInventaris, JenisPeralatan, Merek, Model,
                                                   TglPemasangan, TglKadaluarsa, Status)
                    VALUES (@NoInventaris, @JenisPeralatan, @Merek, @Model,
                            @TglPemasangan, @TglKadaluarsa, ISNULL(@Status, 'Aktif'));
                    
                    SET @InsertedCount = @InsertedCount + 1;
                END
                
                -- Mark processed
                UPDATE dbo.STG_Peralatan
                SET ETL_ProcessedFlag = 1
                WHERE STG_ID = @STG_ID;
                
            END TRY
            BEGIN CATCH
                UPDATE dbo.STG_Peralatan
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE STG_ID = @STG_ID;
                
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            FETCH NEXT FROM staging_cursor INTO @STG_ID, @NoInventaris, @JenisPeralatan,
                  @Merek, @Model, @TglPemasangan, @TglKadaluarsa, @Status;
        END
        
        CLOSE staging_cursor;
        DEALLOCATE staging_cursor;
        
        PRINT 'Dim_Peralatan load completed.';
        PRINT 'Inserted: ' + CAST(@InsertedCount AS VARCHAR(10));
        PRINT 'Updated: ' + CAST(@UpdatedCount AS VARCHAR(10));
        PRINT 'Errors: ' + CAST(@ErrorCount AS VARCHAR(10));
        
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('global', 'staging_cursor') >= 0
        BEGIN
            CLOSE staging_cursor;
            DEALLOCATE staging_cursor;
        END
        
        PRINT 'Error in Dim_Peralatan load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Dim_Peralatan created.';
GO

PRINT 'All dimension load procedures created successfully.';
PRINT '';
GO

-- SECTION 3: FACT LOAD PROCEDURES
PRINT 'Section 3: Creating Fact Load Procedures...';
GO

-- PROCEDURE: usp_Load_Fact_Insiden
-- Memuat data insiden dari staging ke fact table
IF OBJECT_ID('dbo.usp_Load_Fact_Insiden', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Fact_Insiden;
GO

CREATE PROCEDURE dbo.usp_Load_Fact_Insiden
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @RowCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    -- Variables for cursor
    DECLARE @LokasiNama NVARCHAR(200);
    DECLARE @UnitKerjaNama NVARCHAR(200);
    DECLARE @JenisInsidenNama NVARCHAR(100);
    DECLARE @TingkatKeparahanNama NVARCHAR(50);
    DECLARE @TanggalInsiden DATE;
    DECLARE @WaktuInsiden TIME;
    DECLARE @Deskripsi NVARCHAR(MAX);
    DECLARE @KorbanJiwa INT;
    DECLARE @KorbanLukaRingan INT;
    DECLARE @KorbanLukaBerat INT;
    DECLARE @KerugianMateriEstimasi DECIMAL(18,2);
    DECLARE @TindakLanjut NVARCHAR(MAX);
    DECLARE @StatusPenanganan NVARCHAR(50);
    
    -- Surrogate keys
    DECLARE @DateKey INT;
    DECLARE @LokasiKey INT;
    DECLARE @UnitKerjaKey INT;
    DECLARE @JenisInsidenKey INT;
    DECLARE @TingkatKeparahanKey INT;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        DECLARE cur_insiden CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            LokasiNama,
            UnitKerjaNama,
            JenisInsidenNama,
            TingkatKeparahanNama,
            TRY_CAST(TanggalInsiden AS DATE),
            TRY_CAST(WaktuInsiden AS TIME),
            Deskripsi,
            TRY_CAST(KorbanJiwa AS INT),
            TRY_CAST(KorbanLukaRingan AS INT),
            TRY_CAST(KorbanLukaBerat AS INT),
            TRY_CAST(KerugianMateriEstimasi AS DECIMAL(18,2)),
            TindakLanjut,
            StatusPenanganan
        FROM dbo.STG_Insiden
        WHERE IsProcessed = 0;
        
        OPEN cur_insiden;
        
        FETCH NEXT FROM cur_insiden INTO 
            @LokasiNama, @UnitKerjaNama, @JenisInsidenNama, @TingkatKeparahanNama,
            @TanggalInsiden, @WaktuInsiden, @Deskripsi, @KorbanJiwa, @KorbanLukaRingan,
            @KorbanLukaBerat, @KerugianMateriEstimasi, @TindakLanjut, @StatusPenanganan;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validasi data mandatory
                IF @TanggalInsiden IS NULL OR @LokasiNama IS NULL OR @JenisInsidenNama IS NULL
                BEGIN
                    UPDATE dbo.STG_Insiden
                    SET ETL_ErrorMessage = 'Missing mandatory fields: TanggalInsiden, LokasiNama, or JenisInsidenNama'
                    WHERE LokasiNama = @LokasiNama 
                        AND UnitKerjaNama = @UnitKerjaNama 
                        AND JenisInsidenNama = @JenisInsidenNama
                        AND TanggalInsiden = CAST(@TanggalInsiden AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Insiden;
                END
                
                -- Lookup surrogate keys
                EXEC dbo.usp_GetSurrogateKey_Lokasi @LokasiNama, @LokasiKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_UnitKerja @UnitKerjaNama, @UnitKerjaKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_JenisInsiden @JenisInsidenNama, @JenisInsidenKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_TingkatKeparahan @TingkatKeparahanNama, @TingkatKeparahanKey OUTPUT;
                
                -- Get DateKey
                SELECT @DateKey = DateKey 
                FROM dbo.Dim_Date 
                WHERE FullDate = @TanggalInsiden;
                
                -- Validasi semua FK ditemukan
                IF @DateKey IS NULL OR @LokasiKey IS NULL OR @JenisInsidenKey IS NULL
                BEGIN
                    UPDATE dbo.STG_Insiden
                    SET ETL_ErrorMessage = 'Foreign key not found: DateKey=' + ISNULL(CAST(@DateKey AS VARCHAR), 'NULL') + 
                        ', LokasiKey=' + ISNULL(CAST(@LokasiKey AS VARCHAR), 'NULL') +
                        ', JenisInsidenKey=' + ISNULL(CAST(@JenisInsidenKey AS VARCHAR), 'NULL')
                    WHERE LokasiNama = @LokasiNama 
                        AND UnitKerjaNama = @UnitKerjaNama 
                        AND JenisInsidenNama = @JenisInsidenNama
                        AND TanggalInsiden = CAST(@TanggalInsiden AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Insiden;
                END
                
                -- Insert ke Fact_Insiden
                INSERT INTO dbo.Fact_Insiden (
                    DateKey, LokasiKey, UnitKerjaKey, JenisInsidenKey, TingkatKeparahanKey,
                    WaktuInsiden, Deskripsi, KorbanJiwa, KorbanLukaRingan, KorbanLukaBerat,
                    KerugianMateriEstimasi, TindakLanjut, StatusPenanganan
                )
                VALUES (
                    @DateKey, @LokasiKey, @UnitKerjaKey, @JenisInsidenKey, @TingkatKeparahanKey,
                    @WaktuInsiden, @Deskripsi, @KorbanJiwa, @KorbanLukaRingan, @KorbanLukaBerat,
                    @KerugianMateriEstimasi, @TindakLanjut, @StatusPenanganan
                );
                
                -- Mark as processed
                UPDATE dbo.STG_Insiden
                SET IsProcessed = 1, ETL_ProcessedDate = GETDATE()
                WHERE LokasiNama = @LokasiNama 
                    AND UnitKerjaNama = @UnitKerjaNama 
                    AND JenisInsidenNama = @JenisInsidenNama
                    AND TanggalInsiden = CAST(@TanggalInsiden AS NVARCHAR(50))
                    AND IsProcessed = 0;
                
                SET @RowCount = @RowCount + 1;
                
            END TRY
            BEGIN CATCH
                UPDATE dbo.STG_Insiden
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE LokasiNama = @LokasiNama 
                    AND UnitKerjaNama = @UnitKerjaNama 
                    AND JenisInsidenNama = @JenisInsidenNama
                    AND TanggalInsiden = CAST(@TanggalInsiden AS NVARCHAR(50))
                    AND IsProcessed = 0;
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            NextRow_Insiden:
            FETCH NEXT FROM cur_insiden INTO 
                @LokasiNama, @UnitKerjaNama, @JenisInsidenNama, @TingkatKeparahanNama,
                @TanggalInsiden, @WaktuInsiden, @Deskripsi, @KorbanJiwa, @KorbanLukaRingan,
                @KorbanLukaBerat, @KerugianMateriEstimasi, @TindakLanjut, @StatusPenanganan;
        END
        
        CLOSE cur_insiden;
        DEALLOCATE cur_insiden;
        
        COMMIT TRANSACTION;
        
        PRINT 'Fact_Insiden loaded successfully: ' + CAST(@RowCount AS VARCHAR) + ' rows inserted, ' + CAST(@ErrorCount AS VARCHAR) + ' errors.';
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'cur_insiden') >= 0
        BEGIN
            CLOSE cur_insiden;
            DEALLOCATE cur_insiden;
        END
        
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        PRINT 'Error in Fact_Insiden load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Fact_Insiden created.';
GO

-- PROCEDURE: usp_Load_Fact_Inspeksi
-- Memuat data inspeksi dari staging ke fact table
IF OBJECT_ID('dbo.usp_Load_Fact_Inspeksi', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Fact_Inspeksi;
GO

CREATE PROCEDURE dbo.usp_Load_Fact_Inspeksi
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @RowCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    -- Variables for cursor
    DECLARE @LokasiNama NVARCHAR(200);
    DECLARE @UnitKerjaNama NVARCHAR(200);
    DECLARE @PeralatanNama NVARCHAR(200);
    DECLARE @TanggalInspeksi DATE;
    DECLARE @Inspektur NVARCHAR(100);
    DECLARE @HasilInspeksi NVARCHAR(50);
    DECLARE @Catatan NVARCHAR(MAX);
    DECLARE @TemuanMasalah INT;
    DECLARE @TindakanKoreksi NVARCHAR(MAX);
    DECLARE @TanggalTindakLanjut DATE;
    DECLARE @StatusTindakLanjut NVARCHAR(50);
    
    -- Surrogate keys
    DECLARE @DateKey INT;
    DECLARE @LokasiKey INT;
    DECLARE @UnitKerjaKey INT;
    DECLARE @PeralatanKey INT;
    DECLARE @TanggalTindakLanjutKey INT;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        DECLARE cur_inspeksi CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            LokasiNama,
            UnitKerjaNama,
            PeralatanNama,
            TRY_CAST(TanggalInspeksi AS DATE),
            Inspektur,
            HasilInspeksi,
            Catatan,
            TRY_CAST(TemuanMasalah AS INT),
            TindakanKoreksi,
            TRY_CAST(TanggalTindakLanjut AS DATE),
            StatusTindakLanjut
        FROM dbo.STG_Inspeksi
        WHERE IsProcessed = 0;
        
        OPEN cur_inspeksi;
        
        FETCH NEXT FROM cur_inspeksi INTO 
            @LokasiNama, @UnitKerjaNama, @PeralatanNama, @TanggalInspeksi,
            @Inspektur, @HasilInspeksi, @Catatan, @TemuanMasalah, 
            @TindakanKoreksi, @TanggalTindakLanjut, @StatusTindakLanjut;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validasi data mandatory
                IF @TanggalInspeksi IS NULL OR @LokasiNama IS NULL OR @PeralatanNama IS NULL
                BEGIN
                    UPDATE dbo.STG_Inspeksi
                    SET ETL_ErrorMessage = 'Missing mandatory fields: TanggalInspeksi, LokasiNama, or PeralatanNama'
                    WHERE LokasiNama = @LokasiNama 
                        AND PeralatanNama = @PeralatanNama
                        AND TanggalInspeksi = CAST(@TanggalInspeksi AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Inspeksi;
                END
                
                -- Lookup surrogate keys
                EXEC dbo.usp_GetSurrogateKey_Lokasi @LokasiNama, @LokasiKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_UnitKerja @UnitKerjaNama, @UnitKerjaKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_Peralatan @PeralatanNama, @PeralatanKey OUTPUT;
                
                -- Get DateKey
                SELECT @DateKey = DateKey 
                FROM dbo.Dim_Date 
                WHERE FullDate = @TanggalInspeksi;
                
                -- Get TanggalTindakLanjutKey (nullable)
                IF @TanggalTindakLanjut IS NOT NULL
                BEGIN
                    SELECT @TanggalTindakLanjutKey = DateKey 
                    FROM dbo.Dim_Date 
                    WHERE FullDate = @TanggalTindakLanjut;
                END
                ELSE
                BEGIN
                    SET @TanggalTindakLanjutKey = NULL;
                END
                
                -- Validasi FK mandatory
                IF @DateKey IS NULL OR @LokasiKey IS NULL OR @PeralatanKey IS NULL
                BEGIN
                    UPDATE dbo.STG_Inspeksi
                    SET ETL_ErrorMessage = 'Foreign key not found: DateKey=' + ISNULL(CAST(@DateKey AS VARCHAR), 'NULL') + 
                        ', LokasiKey=' + ISNULL(CAST(@LokasiKey AS VARCHAR), 'NULL') +
                        ', PeralatanKey=' + ISNULL(CAST(@PeralatanKey AS VARCHAR), 'NULL')
                    WHERE LokasiNama = @LokasiNama 
                        AND PeralatanNama = @PeralatanNama
                        AND TanggalInspeksi = CAST(@TanggalInspeksi AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Inspeksi;
                END
                
                -- Insert ke Fact_Inspeksi
                INSERT INTO dbo.Fact_Inspeksi (
                    DateKey, LokasiKey, UnitKerjaKey, PeralatanKey, 
                    Inspektur, HasilInspeksi, Catatan, TemuanMasalah,
                    TindakanKoreksi, TanggalTindakLanjutKey, StatusTindakLanjut
                )
                VALUES (
                    @DateKey, @LokasiKey, @UnitKerjaKey, @PeralatanKey,
                    @Inspektur, @HasilInspeksi, @Catatan, @TemuanMasalah,
                    @TindakanKoreksi, @TanggalTindakLanjutKey, @StatusTindakLanjut
                );
                
                -- Mark as processed
                UPDATE dbo.STG_Inspeksi
                SET IsProcessed = 1, ETL_ProcessedDate = GETDATE()
                WHERE LokasiNama = @LokasiNama 
                    AND PeralatanNama = @PeralatanNama
                    AND TanggalInspeksi = CAST(@TanggalInspeksi AS NVARCHAR(50))
                    AND IsProcessed = 0;
                
                SET @RowCount = @RowCount + 1;
                
            END TRY
            BEGIN CATCH
                UPDATE dbo.STG_Inspeksi
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE LokasiNama = @LokasiNama 
                    AND PeralatanNama = @PeralatanNama
                    AND TanggalInspeksi = CAST(@TanggalInspeksi AS NVARCHAR(50))
                    AND IsProcessed = 0;
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            NextRow_Inspeksi:
            FETCH NEXT FROM cur_inspeksi INTO 
                @LokasiNama, @UnitKerjaNama, @PeralatanNama, @TanggalInspeksi,
                @Inspektur, @HasilInspeksi, @Catatan, @TemuanMasalah, 
                @TindakanKoreksi, @TanggalTindakLanjut, @StatusTindakLanjut;
        END
        
        CLOSE cur_inspeksi;
        DEALLOCATE cur_inspeksi;
        
        COMMIT TRANSACTION;
        
        PRINT 'Fact_Inspeksi loaded successfully: ' + CAST(@RowCount AS VARCHAR) + ' rows inserted, ' + CAST(@ErrorCount AS VARCHAR) + ' errors.';
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'cur_inspeksi') >= 0
        BEGIN
            CLOSE cur_inspeksi;
            DEALLOCATE cur_inspeksi;
        END
        
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        PRINT 'Error in Fact_Inspeksi load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Fact_Inspeksi created.';
GO

-- PROCEDURE: usp_Load_Fact_Limbah
-- Memuat data limbah dari staging ke fact table
IF OBJECT_ID('dbo.usp_Load_Fact_Limbah', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Load_Fact_Limbah;
GO

CREATE PROCEDURE dbo.usp_Load_Fact_Limbah
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @RowCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    
    -- Variables for cursor
    DECLARE @LokasiNama NVARCHAR(200);
    DECLARE @UnitKerjaNama NVARCHAR(200);
    DECLARE @JenisLimbahNama NVARCHAR(100);
    DECLARE @TanggalPencatatan DATE;
    DECLARE @VolumeLimbah DECIMAL(18,2);
    DECLARE @SatuanVolume NVARCHAR(20);
    DECLARE @MetodePengelolaan NVARCHAR(100);
    DECLARE @Penanggung_Jawab NVARCHAR(100);
    DECLARE @StatusPengelolaan NVARCHAR(50);
    DECLARE @BiayaPengelolaan DECIMAL(18,2);
    
    -- Surrogate keys
    DECLARE @DateKey INT;
    DECLARE @LokasiKey INT;
    DECLARE @UnitKerjaKey INT;
    DECLARE @JenisLimbahKey INT;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        DECLARE cur_limbah CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            LokasiNama,
            UnitKerjaNama,
            JenisLimbahNama,
            TRY_CAST(TanggalPencatatan AS DATE),
            TRY_CAST(VolumeLimbah AS DECIMAL(18,2)),
            SatuanVolume,
            MetodePengelolaan,
            PenanggungJawab,
            StatusPengelolaan,
            TRY_CAST(BiayaPengelolaan AS DECIMAL(18,2))
        FROM dbo.STG_Limbah
        WHERE IsProcessed = 0;
        
        OPEN cur_limbah;
        
        FETCH NEXT FROM cur_limbah INTO 
            @LokasiNama, @UnitKerjaNama, @JenisLimbahNama, @TanggalPencatatan,
            @VolumeLimbah, @SatuanVolume, @MetodePengelolaan, @Penanggung_Jawab,
            @StatusPengelolaan, @BiayaPengelolaan;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Validasi data mandatory
                IF @TanggalPencatatan IS NULL OR @LokasiNama IS NULL OR @JenisLimbahNama IS NULL
                BEGIN
                    UPDATE dbo.STG_Limbah
                    SET ETL_ErrorMessage = 'Missing mandatory fields: TanggalPencatatan, LokasiNama, or JenisLimbahNama'
                    WHERE LokasiNama = @LokasiNama 
                        AND JenisLimbahNama = @JenisLimbahNama
                        AND TanggalPencatatan = CAST(@TanggalPencatatan AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Limbah;
                END
                
                -- Lookup surrogate keys
                EXEC dbo.usp_GetSurrogateKey_Lokasi @LokasiNama, @LokasiKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_UnitKerja @UnitKerjaNama, @UnitKerjaKey OUTPUT;
                EXEC dbo.usp_GetSurrogateKey_JenisLimbah @JenisLimbahNama, @JenisLimbahKey OUTPUT;
                
                -- Get DateKey
                SELECT @DateKey = DateKey 
                FROM dbo.Dim_Date 
                WHERE FullDate = @TanggalPencatatan;
                
                -- Validasi FK mandatory
                IF @DateKey IS NULL OR @LokasiKey IS NULL OR @JenisLimbahKey IS NULL
                BEGIN
                    UPDATE dbo.STG_Limbah
                    SET ETL_ErrorMessage = 'Foreign key not found: DateKey=' + ISNULL(CAST(@DateKey AS VARCHAR), 'NULL') + 
                        ', LokasiKey=' + ISNULL(CAST(@LokasiKey AS VARCHAR), 'NULL') +
                        ', JenisLimbahKey=' + ISNULL(CAST(@JenisLimbahKey AS VARCHAR), 'NULL')
                    WHERE LokasiNama = @LokasiNama 
                        AND JenisLimbahNama = @JenisLimbahNama
                        AND TanggalPencatatan = CAST(@TanggalPencatatan AS NVARCHAR(50))
                        AND IsProcessed = 0;
                    SET @ErrorCount = @ErrorCount + 1;
                    GOTO NextRow_Limbah;
                END
                
                -- Insert ke Fact_Limbah
                INSERT INTO dbo.Fact_Limbah (
                    DateKey, LokasiKey, UnitKerjaKey, JenisLimbahKey,
                    VolumeLimbah, SatuanVolume, MetodePengelolaan, PenanggungJawab,
                    StatusPengelolaan, BiayaPengelolaan
                )
                VALUES (
                    @DateKey, @LokasiKey, @UnitKerjaKey, @JenisLimbahKey,
                    @VolumeLimbah, @SatuanVolume, @MetodePengelolaan, @Penanggung_Jawab,
                    @StatusPengelolaan, @BiayaPengelolaan
                );
                
                -- Mark as processed
                UPDATE dbo.STG_Limbah
                SET IsProcessed = 1, ETL_ProcessedDate = GETDATE()
                WHERE LokasiNama = @LokasiNama 
                    AND JenisLimbahNama = @JenisLimbahNama
                    AND TanggalPencatatan = CAST(@TanggalPencatatan AS NVARCHAR(50))
                    AND IsProcessed = 0;
                
                SET @RowCount = @RowCount + 1;
                
            END TRY
            BEGIN CATCH
                UPDATE dbo.STG_Limbah
                SET ETL_ErrorMessage = ERROR_MESSAGE()
                WHERE LokasiNama = @LokasiNama 
                    AND JenisLimbahNama = @JenisLimbahNama
                    AND TanggalPencatatan = CAST(@TanggalPencatatan AS NVARCHAR(50))
                    AND IsProcessed = 0;
                SET @ErrorCount = @ErrorCount + 1;
            END CATCH
            
            NextRow_Limbah:
            FETCH NEXT FROM cur_limbah INTO 
                @LokasiNama, @UnitKerjaNama, @JenisLimbahNama, @TanggalPencatatan,
                @VolumeLimbah, @SatuanVolume, @MetodePengelolaan, @Penanggung_Jawab,
                @StatusPengelolaan, @BiayaPengelolaan;
        END
        
        CLOSE cur_limbah;
        DEALLOCATE cur_limbah;
        
        COMMIT TRANSACTION;
        
        PRINT 'Fact_Limbah loaded successfully: ' + CAST(@RowCount AS VARCHAR) + ' rows inserted, ' + CAST(@ErrorCount AS VARCHAR) + ' errors.';
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'cur_limbah') >= 0
        BEGIN
            CLOSE cur_limbah;
            DEALLOCATE cur_limbah;
        END
        
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        PRINT 'Error in Fact_Limbah load: ' + ERROR_MESSAGE();
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_Load_Fact_Limbah created.';
GO

PRINT 'All fact load procedures created successfully.';
PRINT '';
GO


-- SECTION 4: MASTER ETL ORCHESTRATION
PRINT 'Section 4: Creating Master ETL Procedure...';
GO


-- Orchestrates the complete ETL process
IF OBJECT_ID('dbo.usp_ETL_Master', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_ETL_Master;
GO

CREATE PROCEDURE dbo.usp_ETL_Master
    @LoadDimensions BIT = 1,
    @LoadFacts BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ReturnCode INT;
    
    PRINT 'K3L DATA MART - ETL PROCESS STARTED';
    PRINT 'Start Time: ' + CONVERT(VARCHAR, @StartTime, 120);
    PRINT '';
    
    BEGIN TRY
        -- Load Dimensions
        IF @LoadDimensions = 1
        BEGIN
            PRINT 'Loading Dimension Tables...';
            PRINT '';
            
            EXEC @ReturnCode = dbo.usp_Load_Dim_Lokasi;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Dim_Lokasi', 16, 1);
            
            EXEC @ReturnCode = dbo.usp_Load_Dim_UnitKerja;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Dim_UnitKerja', 16, 1);
            
            EXEC @ReturnCode = dbo.usp_Load_Dim_Peralatan;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Dim_Peralatan', 16, 1);
            
            PRINT 'Dimension tables loaded successfully.';
            PRINT '';
        END
        
        -- Load Facts
        IF @LoadFacts = 1
        BEGIN
            PRINT 'Loading Fact Tables...';
            PRINT '';
            
            EXEC @ReturnCode = dbo.usp_Load_Fact_Insiden;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Fact_Insiden', 16, 1);
            
            EXEC @ReturnCode = dbo.usp_Load_Fact_Inspeksi;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Fact_Inspeksi', 16, 1);
            
            EXEC @ReturnCode = dbo.usp_Load_Fact_Limbah;
            IF @ReturnCode <> 0
                RAISERROR('Error loading Fact_Limbah', 16, 1);
            
            PRINT 'Fact tables loaded successfully.';
            PRINT '';
        END
        
        PRINT 'ETL PROCESS COMPLETED SUCCESSFULLY';
        PRINT 'End Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds';
        
        RETURN 0;
        
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        
        PRINT '';
        PRINT 'ETL PROCESS FAILED';
        PRINT 'Error: ' + @ErrorMessage;
        
        RETURN 1;
    END CATCH
END
GO

PRINT 'usp_ETL_Master created.';
GO

-- SECTION 5: VERIFICATION QUERIES
PRINT '';
PRINT 'VERIFICATION - ETL PROCEDURES';
GO

-- Count procedures created
SELECT 
    'ETL Procedures Created' AS VerificationItem,
    COUNT(*) AS Count
FROM sys.procedures
WHERE name LIKE 'usp_Load%' OR name LIKE 'usp_GetSurrogateKey%' OR name = 'usp_ETL_Master';
GO

-- List all procedures
SELECT 
    name AS ProcedureName,
    create_date AS CreatedDate,
    modify_date AS ModifiedDate
FROM sys.procedures
WHERE name LIKE 'usp_%'
ORDER BY name;
GO

PRINT '';
PRINT 'File 07_Create_Procedures.sql completed successfully!';
GO
