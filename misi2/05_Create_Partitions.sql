USE K3L_DataMart;
GO

/* ============================================================
   A. PARTITION FUNCTION & SCHEME
   ============================================================ */

-- Partition Function berdasarkan Tahun
CREATE PARTITION FUNCTION PF_Year (INT)
AS RANGE RIGHT FOR VALUES
(
    2020, -- Tahun 2020
    2021, -- Tahun 2021
    2022, -- Tahun 2022
    2023, -- Tahun 2023
    2024, -- Tahun 2024
    2025  -- Tahun 2025
);
GO

-- Partition Scheme, semua diarahkan ke PRIMARY
CREATE PARTITION SCHEME PS_Year
AS PARTITION PF_Year
ALL TO ([PRIMARY]);
GO


/* ============================================================
   B. FACT TABLES PARTITIONED
   ============================================================ */

-- 1. Fact_Insiden_Partitioned
CREATE TABLE Fact_Insiden_Partitioned (
    InsidenID INT IDENTITY(1,1) NOT NULL,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    JenisInsidenID INT,
    SeverityID INT,
    PetugasID INT,
    JumlahKorban INT,
    HariKerjaHilang INT,
    Tahun INT NOT NULL
) ON PS_Year(Tahun);
GO

-- Clustered Columnstore Index
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Fact_Insiden_Par
ON Fact_Insiden_Partitioned;
GO


-- 2. Fact_Inspeksi_Partitioned
CREATE TABLE Fact_Inspeksi_Partitioned (
    InspeksiID INT IDENTITY(1,1) NOT NULL,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    PeralatanID INT,
    PetugasID INT,
    JumlahDiinspeksi INT,
    JumlahTidakSesuai INT,
    Tahun INT NOT NULL
) ON PS_Year(Tahun);
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Fact_Inspeksi_Par
ON Fact_Inspeksi_Partitioned;
GO


-- 3. Fact_Limbah_Partitioned
CREATE TABLE Fact_Limbah_Partitioned (
    LimbahFactID INT IDENTITY(1,1) NOT NULL,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    LimbahID INT,
    PetugasID INT,
    JumlahLimbah DECIMAL(10,2),
    Tahun INT NOT NULL
) ON PS_Year(Tahun);
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Fact_Limbah_Par
ON Fact_Limbah_Partitioned;
GO
