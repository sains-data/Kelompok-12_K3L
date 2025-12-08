-- Use the database
USE K3L_DataMart;
GO

-----------------------------------------------------------
-- CREATE FACT TABLES - DATA MART K3L ITERA WITH IDENTITY AND INT PK
-----------------------------------------------------------

-- 1. Fact_Insiden
CREATE TABLE Fact_Insiden (
    InsidenID INT IDENTITY(1,1) PRIMARY KEY,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    JenisInsidenID INT,
    SeverityID INT,
    PetugasID INT,
    JumlahKorban INT,
    HariKerjaHilang INT,
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
    FOREIGN KEY (LokasiID) REFERENCES Dim_Lokasi(LokasiID),
    FOREIGN KEY (UnitKerjaID) REFERENCES Dim_UnitKerja(UnitKerjaID),
    FOREIGN KEY (JenisInsidenID) REFERENCES Dim_JenisInsiden(JenisInsidenID),
    FOREIGN KEY (SeverityID) REFERENCES Dim_Severity(SeverityID),
    FOREIGN KEY (PetugasID) REFERENCES Dim_Petugas(PetugasID)
);
GO

-- 2. Fact_Inspeksi
CREATE TABLE Fact_Inspeksi (
    InspeksiID INT IDENTITY(1,1) PRIMARY KEY,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    PeralatanID INT,
    PetugasID INT,
    JumlahDiinspeksi INT,
    JumlahTidakSesuai INT,
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
    FOREIGN KEY (LokasiID) REFERENCES Dim_Lokasi(LokasiID),
    FOREIGN KEY (UnitKerjaID) REFERENCES Dim_UnitKerja(UnitKerjaID),
    FOREIGN KEY (PeralatanID) REFERENCES Dim_JenisPeralatan(PeralatanID),
    FOREIGN KEY (PetugasID) REFERENCES Dim_Petugas(PetugasID)
);
GO

-- 3. Fact_Limbah
CREATE TABLE Fact_Limbah (
    LimbahFactID INT IDENTITY(1,1) PRIMARY KEY,
    DateID INT,
    LokasiID INT,
    UnitKerjaID INT,
    LimbahID INT,
    PetugasID INT,
    JumlahLimbah DECIMAL(10,2),
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
    FOREIGN KEY (LokasiID) REFERENCES Dim_Lokasi(LokasiID),
    FOREIGN KEY (UnitKerjaID) REFERENCES Dim_UnitKerja(UnitKerjaID),
    FOREIGN KEY (LimbahID) REFERENCES Dim_JenisLimbah(LimbahID),
    FOREIGN KEY (PetugasID) REFERENCES Dim_Petugas(PetugasID)
);
GO
