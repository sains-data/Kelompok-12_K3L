USE K3L_DataMart;
GO

/* ============================================================
   1. Load Fact_Insiden_Partitioned
   ============================================================ */
CREATE OR ALTER PROCEDURE usp_Load_FactInsiden
AS
BEGIN
    INSERT INTO Fact_Insiden_Partitioned
    (
        DateID,
        LokasiID,
        UnitKerjaID,
        JenisInsidenID,
        SeverityID,
        PetugasID,
        JumlahKorban,
        HariKerjaHilang,
        Tahun
    )
    SELECT 
        d.DateID,
        l.LokasiID,
        u.UnitKerjaID,
        j.JenisInsidenID,
        s.SeverityID,
        p.PetugasID,
        st.JumlahKorban,
        st.HariKerjaHilang,
        YEAR(st.Tanggal)
    FROM stg.Insiden st
    LEFT JOIN Dim_Date d ON d.Tanggal = st.Tanggal
    LEFT JOIN Dim_Lokasi l ON l.NamaLokasi = st.Lokasi
    LEFT JOIN Dim_UnitKerja u ON u.NamaUnit = st.UnitKerja
    LEFT JOIN Dim_JenisInsiden j ON j.NamaInsiden = st.JenisInsiden
    LEFT JOIN Dim_Severity s ON s.TingkatKeparahan = st.Severity
    LEFT JOIN Dim_Petugas p ON p.NamaPetugas = st.Petugas;
END
GO


/* ============================================================
   2. Load Fact_Inspeksi_Partitioned
   ============================================================ */
CREATE OR ALTER PROCEDURE usp_Load_FactInspeksi
AS
BEGIN
    INSERT INTO Fact_Inspeksi_Partitioned
    (
        DateID,
        LokasiID,
        UnitKerjaID,
        PeralatanID,
        PetugasID,
        JumlahDiinspeksi,
        JumlahTidakSesuai,
        Tahun
    )
    SELECT 
        d.DateID,
        l.LokasiID,
        u.UnitKerjaID,
        jp.PeralatanID,
        p.PetugasID,
        st.JumlahDiinspeksi,
        st.JumlahTidakSesuai,
        YEAR(st.Tanggal)
    FROM stg.Inspeksi st
    LEFT JOIN Dim_Date d ON d.Tanggal = st.Tanggal
    LEFT JOIN Dim_Lokasi l ON l.NamaLokasi = st.Lokasi
    LEFT JOIN Dim_UnitKerja u ON u.NamaUnit = st.UnitKerja
    LEFT JOIN Dim_JenisPeralatan jp ON jp.NamaPeralatan = st.Peralatan
    LEFT JOIN Dim_Petugas p ON p.NamaPetugas = st.Petugas;
END
GO


/* ============================================================
   3. Load Fact_Limbah_Partitioned
   ============================================================ */
CREATE OR ALTER PROCEDURE usp_Load_FactLimbah
AS
BEGIN
    INSERT INTO Fact_Limbah_Partitioned
    (
        DateID,
        LokasiID,
        UnitKerjaID,
        LimbahID,
        PetugasID,
        JumlahLimbah,
        Tahun
    )
    SELECT 
        d.DateID,
        l.LokasiID,
        u.UnitKerjaID,
        jl.LimbahID,
        p.PetugasID,
        st.JumlahLimbah,
        YEAR(st.Tanggal)
    FROM stg.Limbah st
    LEFT JOIN Dim_Date d ON d.Tanggal = st.Tanggal
    LEFT JOIN Dim_Lokasi l ON l.NamaLokasi = st.Lokasi
    LEFT JOIN Dim_UnitKerja u ON u.NamaUnit = st.UnitKerja
    LEFT JOIN Dim_JenisLimbah jl ON jl.JenisLimbah = st.JenisLimbah
    LEFT JOIN Dim_Petugas p ON p.NamaPetugas = st.Petugas;
END
GO


/* ============================================================
   4. Master ETL Procedure
   ============================================================ */
CREATE OR ALTER PROCEDURE usp_Master_ETL
AS
BEGIN
    EXEC usp_Load_FactInsiden;
    EXEC usp_Load_FactInspeksi;
    EXEC usp_Load_FactLimbah;
END
GO
