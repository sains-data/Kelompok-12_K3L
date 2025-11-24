# Analisis Kebutuhan Bisnis (Business Requirements)
# Unit: K3L (Kesehatan, Keselamatan Kerja, dan Lingkungan)

## 1. Identifikasi Stakeholders

* **Pengguna Utama:**
    * Manajer Unit K3L: Membutuhkan dashboard eksekutif untuk memantau KPI utama.
    * Staf K3L (Safety Officer, Environment Officer): Membutuhkan laporan operasional untuk investigasi dan tindak lanjut.
* **Pengguna Sekunder:**
    * Pimpinan ITERA: Membutuhkan ringkasan performa K3L kampus secara keseluruhan.
    * Kepala Unit Kerja/Fakultas: Membutuhkan laporan insiden dan kepatuhan di unit mereka.
    * Auditor Internal: Membutuhkan data untuk keperluan audit kepatuhan.

## 2. Analisis Proses Bisnis

1.  **Pelaporan Insiden:**
    * *Deskripsi:* Karyawan atau mahasiswa melaporkan insiden (kecelakaan, nyaris celaka, tumpahan B3). Staf K3L melakukan investigasi, menentukan penyebab, dan merekomendasikan tindak lanjut.
    * *Data Source:* Sistem E-Reporting Insiden (OLTP), Form Laporan Manual.
2.  **Inspeksi Keselamatan:**
    * *Deskripsi:* Staf K3L melakukan inspeksi rutin terhadap peralatan keselamatan (APAR, Hidran, P3K) dan fasilitas kerja. Temuan (ketidaksesuaian) dicatat dan dipantau hingga selesai (ditutup).
    * *Data Source:* Checklist Inspeksi (Excel/CSV), Aplikasi Mobile Inspeksi.
3.  **Manajemen Limbah:**
    * *Deskripsi:* Unit kerja (Lab, Workshop) menghasilkan limbah. Limbah dikumpulkan, ditimbang/diukur, disimpan di TPS, dan diangkut oleh vendor pihak ketiga.
    * *Data Source:* Logbook Timbangan Limbah (Excel), Dokumen Manifest Vendor.

## 3. Kebutuhan Analitik (Pertanyaan Bisnis)

* Bagaimana tren jumlah insiden (kecelakaan vs nyaris celaka) per bulan?
* Di lokasi (gedung/lab) mana insiden paling sering terjadi?
* Apa penyebab utama (akar masalah) dari kecelakaan kerja?
* Berapa rata-rata hari kerja yang hilang (LTI) akibat kecelakaan?
* Berapa persentase kepatuhan inspeksi APAR per gedung?
* Berapa rata-rata waktu (hari) yang dibutuhkan untuk menutup temuan inspeksi?
* Berapa total volume limbah B3 yang dihasilkan per fakultas setiap kuartal?
* Berapa total biaya pengelolaan limbah per jenis limbah?

## 4. KPI dan Metrik

* **KPI 1:** Menurunkan Angka Kecelakaan Kerja (*Frequency Rate*).
    * *Metrik:* `Jumlah Insiden`, `Jumlah Hari Kerja Hilang`, `Jumlah Jam Kerja Aman`.
* **KPI 2:** Meningkatkan Kepatuhan Inspeksi.
    * *Metrik:* `Jumlah Item Inspeksi`, `Jumlah Temuan`, `Jumlah Temuan Terbuka`, `Waktu Respon Penutupan Temuan`.
* **KPI 3:** Mengoptimalkan Pengelolaan Limbah.
    * *Metrik:* `Volume Limbah (Kg/Liter)`, `Biaya Pengelolaan (IDR)`.