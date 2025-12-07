# Data Sources
# Unit: K3L (Kesehatan, Keselamatan Kerja, dan Lingkungan)

## 1. Daftar Sumber Data

Data Mart K3L mengintegrasikan data dari berbagai sumber operasional yang masuk melalui proses Staging sebelum dimuat ke Data Mart.

| Data Source | Tipe | Entitas Terkait | Keterangan |
| :--- | :--- | :--- | :--- |
| **Sistem E-Reporting Insiden** | OLTP / Form Digital | `Fact_Insiden` | Sumber utama data kejadian kecelakaan, kebakaran, dan insiden lainnya. Mencakup detail korban dan dampak. |
| **Checklist Inspeksi K3** | Excel / App Mobile | `Fact_Inspeksi` | Data hasil inspeksi rutin peralatan (APAR, Hidran) dan fasilitas. Mencatat jumlah item yang diperiksa dan temuan ketidaksesuaian. |
| **Logbook Limbah** | Excel / Catatan Manual | `Fact_Limbah` | Pencatatan harian/mingguan volume limbah yang dihasilkan unit kerja sebelum diangkut vendor. |
| **Sistem Kepegawaian (SIM-SDM)** | Database Master | `Dim_Petugas`, `Dim_UnitKerja` | Data referensi untuk struktur organisasi (Fakultas, Prodi, Unit) dan data personil K3L. |
| **Sistem Sarpras (SIM-Aset)** | Database Master | `Dim_Lokasi`, `Dim_JenisPeralatan` | Data referensi gedung, ruangan, dan inventaris peralatan keselamatan. |

## 2. Alur Data (Data Flow)

1.  **Source Systems** (Excel, OLTP) -> **Staging Area** (`stg.Insiden`, `stg.Inspeksi`, `stg.Limbah`).
2.  **Staging Area** -> **Data Mart** (`Fact_Insiden`, `Fact_Inspeksi`, `Fact_Limbah`) via Stored Procedures (`usp_Master_ETL`).
3.  **Data Mart** -> **Dashboard Views** (`vw_Insiden_Summary`, etc.) -> **Power BI**.

## 3. Spesifikasi Data Staging (Current Implementation)

Saat ini, data dimuat menggunakan skrip SQL (`Insert Data.sql`) yang mensimulasikan data dari sumber-sumber di atas.

### a. Insiden (`stg.Insiden`)
*   **Atribut:** Tanggal, Lokasi, UnitKerja, JenisInsiden, Severity, Petugas, JumlahKorban, HariKerjaHilang.
*   **Frekuensi:** Harian (Transactional).

### b. Inspeksi (`stg.Inspeksi`)
*   **Atribut:** Tanggal, Lokasi, UnitKerja, Peralatan, Petugas, JumlahDiinspeksi, JumlahTidakSesuai.
*   **Frekuensi:** Berkala (Sesuai jadwal inspeksi).

### c. Limbah (`stg.Limbah`)
*   **Atribut:** Tanggal, Lokasi, UnitKerja, JenisLimbah, Petugas, JumlahLimbah.
*   **Frekuensi:** Harian/Mingguan.