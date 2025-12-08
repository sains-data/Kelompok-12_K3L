# Data Mart - Unit K3L (Kesehatan, Keselamatan Kerja, dan Lingkungan)
Tugas Besar Pergudangan Data - Kelompok 12

## Team Members
| NIM | Nama |
|---|---|
| 123450063 | Arya Muda Siregar |
| 122450129 | Vira Putri Maharani |
| 122450021 | Lisa Diani Amelia |
| 123450111 | Zailani Satria |

## Project Description
Proyek ini bertujuan untuk merancang dan membangun Data Mart untuk Unit K3L ITERA. Solusi ini mengintegrasikan data dari berbagai sumber operasional (pelaporan insiden, inspeksi, dan manajemen limbah) ke dalam satu repositori terpusat untuk mendukung pengambilan keputusan berbasis data terkait keselamatan kerja dan kepatuhan lingkungan.

## Business Domain
Unit K3L bertanggung jawab atas pengelolaan aspek Kesehatan, Keselamatan Kerja, dan Lingkungan di seluruh area kampus.
- **Fungsi Utama:** Investigasi kecelakaan kerja, inspeksi rutin peralatan keselamatan (APAR/Hidran), dan pengelolaan limbah B3/Non-B3.
- **Tujuan:** Meminimalkan risiko kecelakaan (Zero Accident), memastikan kepatuhan terhadap regulasi, dan menjaga kelestarian lingkungan kampus.

## Architecture
- **Approach:** Kimball Dimensional Modeling (Bottom-up Star Schema)
- **Platform:** SQL Server 2022
- **ETL:** SQL Stored Procedures (ELT Pattern) orchestrated by SQL Server Agent

## Key Features
- **Fact Tables:**
  - `Fact_Insiden`: Mencatat detail kejadian kecelakaan dan dampaknya.
  - `Fact_Inspeksi`: Melacak hasil pemeriksaan peralatan dan fasilitas.
  - `Fact_Limbah`: Memantau volume dan jenis limbah yang dihasilkan.
- **Dimension Tables:**
  - `Dim_Date`, `Dim_Lokasi`, `Dim_UnitKerja`, `Dim_Petugas`
  - `Dim_JenisInsiden`, `Dim_Severity`, `Dim_JenisPeralatan`, `Dim_JenisLimbah`
- **KPIs:**
  - Frequency Rate (Tingkat Kekerapan Insiden)
  - Compliance Rate (Tingkat Kepatuhan Inspeksi)
  - Waste Volume Analysis (Analisis Volume Limbah)

## Documentation
- [Business Requirements](docs/01-requirements/business_requirements.md)
- [Data Sources](docs/01-requirements/data_sources.md)
- [Design Documents](docs/02-design/)

## Timeline
- **Misi 1:** 17 November 2025
- **Misi 2:** 24 November 2025
- **Misi 3:** 01 December 2025