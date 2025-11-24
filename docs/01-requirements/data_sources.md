# Data Sources
# Unit: K3L (Kesehatan, Keselamatan Kerja, dan Lingkungan)

## 1. Daftar Sumber Data

Berikut adalah identifikasi sumber data utama yang akan digunakan untuk mengisi data mart K3L.

| Data Source | Tipe | Perkiraan Volume | Frekuensi Update | Kualitas Data | Catatan |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Sistem E-Reporting Insiden** | Database OLTP (SQL Server) | 100-200 insiden/tahun | Real-time | Cukup Baik | Sumber utama untuk `Fact_Insiden`. Perlu join untuk detail. |
| **Checklist Inspeksi APAR** | Excel / CSV | ~50 file/bulan | Bulanan | Sedang | Data sering manual, perlu *cleansing* format tanggal & nama. |
| **Checklist Inspeksi Hidran** | Excel / CSV | ~10 file/kuartal | Kuartalan | Sedang | Mirip dengan APAR, struktur file mungkin berbeda. |
| **Logbook Limbah B3** | Excel / Manual | 1 file/minggu | Mingguan | Rendah | Sangat manual, risiko salah ketik tinggi. Perlu validasi. |
| **Data Vendor Limbah** | PDF / Excel | 1 file/bulan | Bulanan | Baik | Data terstruktur dari vendor (pihak ketiga) untuk biaya. |
| **Database SIM-Kepegawaian** | Database OLTP (SQL Server) | ~1.000+ pegawai | Harian | Tinggi | Digunakan untuk dimensi `Dim_Inspektur` dan `Dim_UnitKerja`. |
| **Database SIM-Sarpras** | Database OLTP (SQL Server) | ~500+ ruangan | Sesuai Kebutuhan | Tinggi | Digunakan untuk data master `Dim_Lokasi` (Gedung, Ruangan). |

## 2. Profiling & Analisis Data (Contoh)

### Sumber: Checklist Inspeksi APAR (Excel)

* **Struktur:** 1 file Excel per gedung, 1 *sheet* per lantai, 1 baris per APAR.
* **Kolom Kunci:** `ID_APAR`, `Lokasi`, `Tgl_Inspeksi`, `Kondisi_Tabung`, `Kondisi_Pin`, `Kondisi_Selang`, `Tekanan`, `Temuan`, `Nama_Inspektur`.
* **Masalah Kualitas:**
    * **Konsistensi:** `Tgl_Inspeksi` sering ditulis dalam format berbeda (DD/MM/YY, MM/DD/YYYY, Teks).
    * **Akurasi:** `Nama_Inspektur` kadang typo atau disingkat (misal: "Budi S." vs "Budi Setiawan").
    * **Kelengkapan:** Kolom `Temuan` sering kosong padahal kondisi "Tidak Baik".
* **Rencana Transformasi (ETL):**
    * Standardisasi format tanggal ke `YYYY-MM-DD`.
    * Gunakan *Lookup Table* (dari `Dim_Inspektur`) untuk membersihkan nama inspektur.
    * Buat aturan *derived column*: JIKA `Kondisi_Tabung` = 'Tidak Baik' MAKA `JumlahTemuan` = 1.

### Sumber: Sistem E-Reporting Insiden (OLTP)

* **Struktur:** Tabel relasional (misal: `tbl_insiden`, `tbl_investigasi`, `tbl_korban`).
* **Masalah Kualitas:**
    * **NULL Values:** `Tingkat_Keparahan` kadang NULL dan baru diisi setelah investigasi selesai.
    * **Kategorisasi:** `Jenis_Insiden` tidak konsisten (misal: "Kecelakaan" vs "Kecelakaan Kerja").
* **Rencana Transformasi (ETL):**
    * Gunakan `COALESCE` untuk mengisi nilai NULL dengan 'Belum Diinvestigasi'.
    * Terapkan *mapping* untuk menstandardisasi `Jenis_Insiden` ke `Dim_JenisInsiden`.