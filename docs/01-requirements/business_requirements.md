# Analisis Kebutuhan Bisnis (Business Requirements)
# Unit: K3L (Kesehatan, Keselamatan Kerja, dan Lingkungan)

## 1. Identifikasi Stakeholders

* **Pengguna Utama:**
    * **Manajer Unit K3L:** Membutuhkan dashboard eksekutif untuk memantau KPI utama (Total Insiden, Korban, Kepatuhan).
    * **Staf K3L:** Membutuhkan laporan operasional untuk investigasi insiden dan tindak lanjut inspeksi.
    * **Supervisor & Koordinator K3L:** Memantau kinerja tim dan area spesifik.
* **Pengguna Sekunder:**
    * **Pimpinan ITERA (Rektor/Wakil Rektor):** Membutuhkan ringkasan performa K3L kampus secara keseluruhan (Executive Summary).
    * **Kepala Unit Kerja/Fakultas (Dekan/Direktur):** Membutuhkan laporan insiden dan kepatuhan di unit akademik/penunjang mereka.
    * **Auditor Internal:** Membutuhkan data historis untuk keperluan audit kepatuhan dan keselamatan.

## 2. Analisis Proses Bisnis

1.  **Pelaporan Insiden:**
    *   *Deskripsi:* Pencatatan kejadian insiden (kecelakaan, kebakaran, tumpahan B3, dll) yang terjadi di lingkungan kampus.
    *   *Data Point:* Tanggal, Lokasi, Unit Kerja, Jenis Insiden, Tingkat Keparahan, Jumlah Korban, Hari Kerja Hilang.
    *   *Tabel:* `Fact_Insiden`.
2.  **Inspeksi Keselamatan:**
    *   *Deskripsi:* Kegiatan inspeksi rutin terhadap peralatan K3 (APAR, Hidran, dll) dan fasilitas.
    *   *Data Point:* Tanggal, Lokasi, Unit Kerja, Jenis Peralatan, Jumlah Diinspeksi, Jumlah Temuan (Tidak Sesuai).
    *   *Tabel:* `Fact_Inspeksi`.
3.  **Manajemen Limbah:**
    *   *Deskripsi:* Pemantauan volume limbah yang dihasilkan oleh setiap unit kerja berdasarkan jenisnya.
    *   *Data Point:* Tanggal, Lokasi, Unit Kerja, Jenis Limbah, Jumlah Limbah (Kg/Liter).
    *   *Tabel:* `Fact_Limbah`.

## 3. Kebutuhan Analitik (Pertanyaan Bisnis)

*   **Executive Summary:**
    *   Berapa total insiden, korban, dan hari kerja hilang tahun ini?
    *   Bagaimana tren total insiden selama 5 tahun terakhir?
    *   Unit kerja mana yang memiliki jumlah korban terbanyak?
*   **Academic Performance (Kepatuhan):**
    *   Bagaimana tingkat kepatuhan inspeksi (Persentase Sesuai vs Tidak Sesuai) per unit kerja?
    *   Apa peralatan yang paling sering ditemukan tidak sesuai standar?
    *   Bagaimana sebaran tingkat keparahan insiden di setiap fakultas/unit?
*   **Operational & Impact:**
    *   Berapa total volume limbah yang dihasilkan per kategori (B3 vs Non-B3)?
    *   Bagaimana proporsi jenis insiden yang terjadi (misal: Kecelakaan vs Kebakaran)?
    *   Apakah ada korelasi antara volume limbah dan insiden di unit tertentu?

## 4. KPI dan Metrik

*   **KPI 1: Keselamatan Kerja (Safety)**
    *   *Metrik:* `Total Insiden`, `Total Korban`, `Total Hari Kerja Hilang` (Severity Rate proxy).
*   **KPI 2: Kepatuhan (Compliance)**
    *   *Metrik:* `Total Inspeksi`, `Total Temuan (Tidak Sesuai)`, `Persentase Ketidaksesuaian` (Rasio Temuan/Inspeksi).
*   **KPI 3: Lingkungan (Environment)**
    *   *Metrik:* `Total Volume Limbah` (per Jenis/Kategori).