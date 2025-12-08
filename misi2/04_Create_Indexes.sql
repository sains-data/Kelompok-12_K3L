USE K3L_DataMart;
GO

/* ============================================================
   A. INDEX FOR FACT_INSIDEN
   ============================================================ */

-- Foreign Key Indexes
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Date
ON Fact_Insiden (DateID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Lokasi
ON Fact_Insiden (LokasiID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Insiden_UnitKerja
ON Fact_Insiden (UnitKerjaID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Insiden_JenisInsiden
ON Fact_Insiden (JenisInsidenID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Severity
ON Fact_Insiden (SeverityID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Petugas
ON Fact_Insiden (PetugasID);
GO

-- Covering Index
CREATE NONCLUSTERED INDEX IX_Fact_Insiden_Covering
ON Fact_Insiden (DateID, UnitKerjaID, JenisInsidenID)
INCLUDE (JumlahKorban, HariKerjaHilang, SeverityID);
GO

-- Columnstore Index
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_Insiden
ON Fact_Insiden
(
    DateID, LokasiID, UnitKerjaID, JenisInsidenID, SeverityID,
    PetugasID, JumlahKorban, HariKerjaHilang
);
GO



/* ============================================================
   B. INDEX FOR FACT_INSPEKSI
   ============================================================ */

-- Foreign Key Indexes
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Date
ON Fact_Inspeksi (DateID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Lokasi
ON Fact_Inspeksi (LokasiID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_UnitKerja
ON Fact_Inspeksi (UnitKerjaID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Peralatan
ON Fact_Inspeksi (PeralatanID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Petugas
ON Fact_Inspeksi (PetugasID);
GO

-- Covering Index
CREATE NONCLUSTERED INDEX IX_Fact_Inspeksi_Covering
ON Fact_Inspeksi (DateID, UnitKerjaID)
INCLUDE (JumlahDiinspeksi, JumlahTidakSesuai, PeralatanID);
GO

-- Columnstore Index
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_Inspeksi
ON Fact_Inspeksi
(
    DateID, LokasiID, UnitKerjaID, PeralatanID, PetugasID,
    JumlahDiinspeksi, JumlahTidakSesuai
);
GO



/* ============================================================
   C. INDEX FOR FACT_LIMBAH
   ============================================================ */

-- Foreign Key Indexes
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Date
ON Fact_Limbah (DateID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Lokasi
ON Fact_Limbah (LokasiID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Limbah_UnitKerja
ON Fact_Limbah (UnitKerjaID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Limbah
ON Fact_Limbah (LimbahID);
GO

CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Petugas
ON Fact_Limbah (PetugasID);
GO

-- Covering Index
CREATE NONCLUSTERED INDEX IX_Fact_Limbah_Covering
ON Fact_Limbah (DateID, UnitKerjaID, LimbahID)
INCLUDE (JumlahLimbah, LokasiID);
GO

-- Columnstore Index
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX_Fact_Limbah
ON Fact_Limbah
(
    DateID, LokasiID, UnitKerjaID, LimbahID, PetugasID,
    JumlahLimbah
);
GO
