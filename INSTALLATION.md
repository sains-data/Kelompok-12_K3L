# Panduan Instalasi K3L Data Mart

## Daftar Isi
1. [Prerequisites](#prerequisites)
2. [Persiapan Environment](#persiapan-environment)
3. [Instalasi Database](#instalasi-database)
4. [Setup Airflow](#setup-airflow)
5. [Konfigurasi](#konfigurasi)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

---

## 1. Prerequisites

### 1.1 Hardware Requirements

| Komponen | Minimum | Recommended |
|----------|---------|-------------|
| **CPU** | 2 cores | 4 cores |
| **RAM** | 4 GB | 8 GB |
| **Disk Space** | 20 GB | 50 GB |
| **Network** | 100 Mbps | 1 Gbps |

### 1.2 Software Requirements

| Software | Versi | Keterangan |
|----------|-------|-----------|
| **OS** | Ubuntu 20.04+ | VM atau bare metal |
| **SQL Server** | 2022 | SQL Server on Linux |
| **Python** | 3.8+ | Untuk Airflow dan ETL scripts |
| **Apache Airflow** | 2.7+ | Orchestration tool |
| **Git** | 2.25+ | Version control |

### 1.3 User Permissions

Pastikan Anda memiliki:
- [ ] Akses `sudo` di Ubuntu
- [ ] Akses `sa` atau `sysadmin` di SQL Server
- [ ] Akses write ke `/var/opt/mssql/data/` dan `/data/k3l/`

---

## 2. Persiapan Environment

### 2.1 Update System

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl wget vim
```

### 2.2 Verifikasi SQL Server

```bash
# Check SQL Server status
sudo systemctl status mssql-server

# Check SQL Server version
sqlcmd -S localhost -U sa -Q "SELECT @@VERSION"
```

**Expected output:**
```
Microsoft SQL Server 2022 (RTM-CU...) on Linux (Ubuntu 20.04...)
```

### 2.3 Clone Repository

```bash
cd ~
git clone https://github.com/your-org/k3l-datamart.git k3l
cd k3l
```

**Struktur folder:**
```
k3l/
├── sql/
│   ├── 01_Create_Database.sql
│   ├── 02_Create_Dimensions.sql
│   ├── 03_Create_Facts.sql
│   ├── 04_Create_Indexes.sql
│   ├── 05_Create_Partitions.sql
│   ├── 06_Create_Staging.sql
│   ├── 07_Create_Procedures.sql
│   └── 08_DataQuality_Checks.sql
├── etl/
│   ├── scripts/
│   └── packages/
├── docs/
└── README.md
```

### 2.4 Setup Python Environment

```bash
# Install Python 3 dan pip
sudo apt install -y python3 python3-pip python3-venv

# Create virtual environment
cd ~/k3l
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip
```

### 2.5 Install Python Dependencies

```bash
# Create requirements.txt (jika belum ada)
cat > requirements.txt <<EOF
apache-airflow==2.7.3
apache-airflow-providers-microsoft-mssql==3.4.0
pandas==2.1.0
openpyxl==3.1.2
pyodbc==5.0.1
python-dotenv==1.0.0
EOF

# Install dependencies
pip install -r requirements.txt
```

---

## 3. Instalasi Database

### 3.1 Persiapan SQL Server

#### 3.1.1 Set SA Password (jika belum)

```bash
sudo /opt/mssql/bin/mssql-conf set-sa-password
```

Masukkan password yang kuat (minimal 8 karakter, uppercase, lowercase, digit, special char).

#### 3.1.2 Enable SQL Server Agent

```bash
sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true
sudo systemctl restart mssql-server
```

#### 3.1.3 Verifikasi Connection

```bash
sqlcmd -S localhost -U sa
# Masukkan password
# Jika sukses, muncul prompt: 1>
# Ketik: SELECT 1;
# Ketik: GO
# Ketik: EXIT
```

### 3.2 Eksekusi SQL Scripts

**PENTING:** Jalankan script sesuai urutan berikut:

```bash
cd ~/k3l/sql

# Set environment variables
export SQLCMDSERVER=localhost
export SQLCMDUSER=sa
export SQLCMDPASSWORD='YourSAPassword'  # Ganti dengan password Anda
export SQLCMDDBNAME=master
```

#### 3.2.1 Step 1: Create Database

```bash
sqlcmd -i 01_Create_Database.sql -o /tmp/01_output.log
echo "Step 1 completed. Check /tmp/01_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -Q "SELECT name FROM sys.databases WHERE name = 'K3L_DataMart'"
```

Expected: `K3L_DataMart`

#### 3.2.2 Step 2: Create Dimensions

```bash
sqlcmd -d K3L_DataMart -i 02_Create_Dimensions.sql -o /tmp/02_output.log
echo "Step 2 completed. Check /tmp/02_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS DimensionCount FROM sys.tables WHERE name LIKE 'Dim_%'"
```

Expected: `7` (7 dimension tables)

#### 3.2.3 Step 3: Create Facts

```bash
sqlcmd -d K3L_DataMart -i 03_Create_Facts.sql -o /tmp/03_output.log
echo "Step 3 completed. Check /tmp/03_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS FactCount FROM sys.tables WHERE name LIKE 'Fact_%'"
```

Expected: `3` (3 fact tables)

#### 3.2.4 Step 4: Create Partitions

**⚠️ WARNING:** Step ini akan DROP dan RECREATE fact tables. Jangan jalankan jika sudah ada data!

```bash
# Backup dulu jika ada data
sqlcmd -d K3L_DataMart -Q "BACKUP DATABASE K3L_DataMart TO DISK = '/var/opt/mssql/data/K3L_DataMart_before_partition.bak'"

# Run partitioning
sqlcmd -d K3L_DataMart -i 05_Create_Partitions.sql -o /tmp/05_output.log
echo "Step 4 completed. Check /tmp/05_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS PartitionCount FROM sys.partition_schemes WHERE name = 'PS_DateKey_Quarterly'"
```

Expected: `1`

#### 3.2.5 Step 5: Create Indexes

```bash
sqlcmd -d K3L_DataMart -i 04_Create_Indexes.sql -o /tmp/04_output.log
echo "Step 5 completed. Check /tmp/04_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS IndexCount FROM sys.indexes WHERE name LIKE 'IX_%' OR name LIKE 'UIX_%'"
```

Expected: `~35` indexes

#### 3.2.6 Step 6: Create Staging

```bash
sqlcmd -d K3L_DataMart -i 06_Create_Staging.sql -o /tmp/06_output.log
echo "Step 6 completed. Check /tmp/06_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS StagingCount FROM sys.tables WHERE name LIKE 'STG_%'"
```

Expected: `6` staging tables

#### 3.2.7 Step 7: Create Procedures

```bash
sqlcmd -d K3L_DataMart -i 07_Create_Procedures.sql -o /tmp/07_output.log
echo "Step 7 completed. Check /tmp/07_output.log for errors."
```

**Verifikasi:**
```bash
sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) AS ProcedureCount FROM sys.procedures WHERE name LIKE 'usp_%'"
```

Expected: `11` stored procedures

#### 3.2.8 Check All Logs

```bash
# Check for errors in all logs
grep -i "error\|fail" /tmp/0*.log
```

Jika tidak ada output, berarti instalasi sukses!

### 3.3 Populate Dim_Date

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Generate dates from 2023-01-01 to 2030-12-31
DECLARE @StartDate DATE = '2023-01-01';
DECLARE @EndDate DATE = '2030-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dbo.Dim_Date (
        DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
        WeekOfYear, MonthNumber, MonthName, Quarter, QuarterName, Year, IsWeekend
    )
    VALUES (
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
        @StartDate,
        DATEPART(WEEKDAY, @StartDate),
        DATENAME(WEEKDAY, @StartDate),
        DATEPART(DAY, @StartDate),
        DATEPART(DAYOFYEAR, @StartDate),
        DATEPART(WEEK, @StartDate),
        DATEPART(MONTH, @StartDate),
        DATENAME(MONTH, @StartDate),
        DATEPART(QUARTER, @StartDate),
        'Q' + CAST(DATEPART(QUARTER, @StartDate) AS VARCHAR) + '-' + CAST(YEAR(@StartDate) AS VARCHAR),
        YEAR(@StartDate),
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
    );
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END
GO

SELECT COUNT(*) AS Dim_Date_RowCount FROM dbo.Dim_Date;
GO
EOF
```

Expected: `~2922` rows (2023-2030)

### 3.4 Populate Static Dimensions

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Dim_TingkatKeparahan
INSERT INTO dbo.Dim_TingkatKeparahan (NamaTingkatKeparahan, Deskripsi) VALUES
('Ringan', 'Insiden dengan dampak minimal, tidak ada korban jiwa atau luka'),
('Sedang', 'Insiden dengan korban luka ringan atau kerugian material sedang'),
('Berat', 'Insiden dengan korban luka berat atau kerugian material signifikan'),
('Fatal', 'Insiden dengan korban jiwa atau kerugian material sangat besar');

-- Dim_JenisInsiden
INSERT INTO dbo.Dim_JenisInsiden (NamaJenisInsiden, Kategori, Deskripsi) VALUES
('Kebakaran', 'Kebakaran', 'Insiden kebakaran di area kampus'),
('Kecelakaan Kerja', 'Keselamatan', 'Kecelakaan saat bekerja di laboratorium atau bengkel'),
('Tumpahan Bahan Kimia', 'Lingkungan', 'Tumpahan bahan kimia berbahaya'),
('Kecelakaan Lalu Lintas', 'Keselamatan', 'Kecelakaan kendaraan di area kampus'),
('Kebocoran Gas', 'Keselamatan', 'Kebocoran gas di laboratorium'),
('Ledakan', 'Keselamatan', 'Ledakan peralatan atau bahan kimia'),
('Cedera Olahraga', 'Kesehatan', 'Cedera saat kegiatan olahraga'),
('Keracunan', 'Kesehatan', 'Keracunan makanan atau bahan kimia');

-- Dim_JenisLimbah
INSERT INTO dbo.Dim_JenisLimbah (NamaJenisLimbah, KategoriLimbah, StatusB3, Deskripsi) VALUES
('Limbah Padat Non-B3', 'Padat', 0, 'Limbah padat rumah tangga (kertas, plastik, dll)'),
('Limbah Padat B3', 'Padat', 1, 'Limbah padat berbahaya (lampu neon, baterai, dll)'),
('Limbah Cair Non-B3', 'Cair', 0, 'Air limbah domestik'),
('Limbah Cair B3', 'Cair', 1, 'Limbah cair dari laboratorium mengandung bahan kimia'),
('Limbah Infeksius', 'Medis', 1, 'Limbah dari klinik kampus (jarum suntik, perban)'),
('Limbah Elektronik', 'Elektronik', 1, 'E-waste (komputer, printer, kabel)'),
('Limbah Organik', 'Organik', 0, 'Sampah dapur, dedaunan');

SELECT 'Dim_TingkatKeparahan' AS TableName, COUNT(*) AS RowCount FROM Dim_TingkatKeparahan
UNION ALL
SELECT 'Dim_JenisInsiden', COUNT(*) FROM Dim_JenisInsiden
UNION ALL
SELECT 'Dim_JenisLimbah', COUNT(*) FROM Dim_JenisLimbah;
GO
EOF
```

---

## 4. Setup Airflow

### 4.1 Initialize Airflow

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow
echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc

# Initialize database (SQLite untuk development)
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname K3L \
    --role Admin \
    --email admin@itera.ac.id \
    --password admin123
```

### 4.2 Configure Airflow

```bash
# Edit airflow.cfg
vim $AIRFLOW_HOME/airflow.cfg
```

**Ubah settings berikut:**
```ini
[core]
dags_folder = /home/arya/k3l/etl/dags
load_examples = False

[webserver]
web_server_port = 8080

[scheduler]
catchup_by_default = False
```

### 4.3 Create Airflow DAGs Folder

```bash
mkdir -p ~/k3l/etl/dags
mkdir -p ~/k3l/etl/scripts
mkdir -p /data/k3l/sources/{insiden,inspeksi,limbah,master_data}
```

### 4.4 Setup Connection ke SQL Server

```bash
# Via CLI
airflow connections add k3l_db \
    --conn-type mssql \
    --conn-host localhost \
    --conn-schema K3L_DataMart \
    --conn-login sa \
    --conn-password 'YourSAPassword' \
    --conn-port 1433
```

**Atau via Airflow UI:**
1. Buka http://localhost:8080
2. Login dengan `admin` / `admin123`
3. Menu: Admin → Connections → Add a new record
4. Isi:
   - Conn Id: `k3l_db`
   - Conn Type: `Microsoft SQL Server`
   - Host: `localhost`
   - Schema: `K3L_DataMart`
   - Login: `sa`
   - Password: `YourSAPassword`
   - Port: `1433`

### 4.5 Create Sample DAG

```bash
cat > ~/k3l/etl/dags/k3l_etl_master.py <<'EOF'
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'k3l_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k3l_etl_master',
    default_args=default_args,
    description='K3L Data Mart ETL Master',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['k3l', 'etl']
) as dag:
    
    load_dimensions = MsSqlOperator(
        task_id='load_dimensions',
        mssql_conn_id='k3l_db',
        sql="""
        EXEC dbo.usp_Load_Dim_Lokasi;
        EXEC dbo.usp_Load_Dim_UnitKerja;
        EXEC dbo.usp_Load_Dim_Peralatan;
        """
    )
    
    load_facts = MsSqlOperator(
        task_id='load_facts',
        mssql_conn_id='k3l_db',
        sql="""
        EXEC dbo.usp_Load_Fact_Insiden;
        EXEC dbo.usp_Load_Fact_Inspeksi;
        EXEC dbo.usp_Load_Fact_Limbah;
        """
    )
    
    load_dimensions >> load_facts

EOF
```

### 4.6 Start Airflow Services

```bash
# Terminal 1: Start webserver
airflow webserver -p 8080

# Terminal 2: Start scheduler
airflow scheduler
```

**Atau gunakan systemd service (production):**

```bash
# Create systemd service files
sudo tee /etc/systemd/system/airflow-webserver.service > /dev/null <<EOF
[Unit]
Description=Airflow webserver
After=network.target

[Service]
Environment="AIRFLOW_HOME=/home/arya/airflow"
User=arya
Group=arya
Type=simple
ExecStart=/home/arya/k3l/venv/bin/airflow webserver -p 8080
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/airflow-scheduler.service > /dev/null <<EOF
[Unit]
Description=Airflow scheduler
After=network.target

[Service]
Environment="AIRFLOW_HOME=/home/arya/airflow"
User=arya
Group=arya
Type=simple
ExecStart=/home/arya/k3l/venv/bin/airflow scheduler
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver airflow-scheduler
sudo systemctl start airflow-webserver airflow-scheduler
```

---

## 5. Konfigurasi

### 5.1 Database User untuk ETL

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Create ETL user (optional, untuk production)
CREATE LOGIN k3l_etl_user WITH PASSWORD = 'EtlUser@2024';
CREATE USER k3l_etl_user FOR LOGIN k3l_etl_user;

-- Grant permissions
ALTER ROLE db_datareader ADD MEMBER k3l_etl_user;
ALTER ROLE db_datawriter ADD MEMBER k3l_etl_user;
GRANT EXECUTE TO k3l_etl_user;
GO
EOF
```

### 5.2 Environment Variables

```bash
# Create .env file
cat > ~/k3l/.env <<EOF
# Database
DB_SERVER=localhost
DB_NAME=K3L_DataMart
DB_USER=sa
DB_PASSWORD=YourSAPassword

# Airflow
AIRFLOW_HOME=/home/arya/airflow

# Data sources
DATA_SOURCE_PATH=/data/k3l/sources
EOF

# Load .env in bashrc
echo "source ~/k3l/.env" >> ~/.bashrc
source ~/.bashrc
```

### 5.3 Firewall (jika perlu)

```bash
# Allow SQL Server port
sudo ufw allow 1433/tcp

# Allow Airflow port
sudo ufw allow 8080/tcp
```

---

## 6. Testing

### 6.1 Database Connection Test

```bash
# Python test
python3 <<EOF
import pyodbc

conn_str = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=K3L_DataMart;'
    'UID=sa;'
    'PWD=YourSAPassword;'
    'TrustServerCertificate=yes;'
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM Dim_Date")
    row = cursor.fetchone()
    print(f"Success! Dim_Date has {row[0]} rows.")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
EOF
```

### 6.2 Stored Procedure Test

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Test helper procedure
DECLARE @LokasiKey INT;
EXEC dbo.usp_GetSurrogateKey_Lokasi 'Test Lokasi', @LokasiKey OUTPUT;
SELECT @LokasiKey AS TestLokasiKey;

-- Test master ETL (dry run tanpa data)
EXEC dbo.usp_ETL_Master @LoadDimensions=1, @LoadFacts=1;
GO
EOF
```

### 6.3 Airflow DAG Test

```bash
# List DAGs
airflow dags list

# Test DAG structure
airflow dags test k3l_etl_master 2024-01-01

# Trigger DAG manually
airflow dags trigger k3l_etl_master
```

### 6.4 Data Quality Test

```bash
sqlcmd -d K3L_DataMart -i ~/k3l/sql/08_DataQuality_Checks.sql -o /tmp/dq_results.log
cat /tmp/dq_results.log
```

---

## 7. Troubleshooting

### 7.1 SQL Server Issues

**Problem:** Cannot connect to SQL Server
```bash
# Check service status
sudo systemctl status mssql-server

# Restart service
sudo systemctl restart mssql-server

# Check logs
sudo tail -f /var/opt/mssql/log/errorlog
```

**Problem:** Permission denied on `/var/opt/mssql/data/`
```bash
sudo chown -R mssql:mssql /var/opt/mssql/data/
sudo chmod 755 /var/opt/mssql/data/
```

### 7.2 Airflow Issues

**Problem:** Airflow webserver tidak bisa diakses
```bash
# Check if port is listening
sudo netstat -tuln | grep 8080

# Check webserver logs
tail -f ~/airflow/logs/scheduler/latest/*.log
```

**Problem:** DAG tidak muncul di UI
```bash
# Check DAG file syntax
python ~/k3l/etl/dags/k3l_etl_master.py

# Check Airflow logs
airflow dags list-import-errors
```

### 7.3 Python Issues

**Problem:** ModuleNotFoundError
```bash
# Activate venv
source ~/k3l/venv/bin/activate

# Reinstall dependencies
pip install -r ~/k3l/requirements.txt
```

### 7.4 Common Errors

| Error | Solution |
|-------|----------|
| "Login failed for user 'sa'" | Check password, reset with `mssql-conf set-sa-password` |
| "Database 'K3L_DataMart' does not exist" | Run `01_Create_Database.sql` |
| "Invalid object name 'dbo.Dim_Date'" | Run `02_Create_Dimensions.sql` |
| "Procedure 'usp_ETL_Master' not found" | Run `07_Create_Procedures.sql` |
| "Partition function not found" | Run `05_Create_Partitions.sql` |

---

## Checklist Instalasi

Gunakan checklist ini untuk memastikan semua step sudah dilakukan:

- [ ] Ubuntu 20.04+ installed
- [ ] SQL Server 2022 installed dan running
- [ ] Python 3.8+ dan venv setup
- [ ] Repository cloned
- [ ] Python dependencies installed
- [ ] SQL Server SA password set
- [ ] SQL Server Agent enabled
- [ ] Database created (01_Create_Database.sql)
- [ ] Dimensions created (02_Create_Dimensions.sql)
- [ ] Facts created (03_Create_Facts.sql)
- [ ] Partitions created (05_Create_Partitions.sql)
- [ ] Indexes created (04_Create_Indexes.sql)
- [ ] Staging tables created (06_Create_Staging.sql)
- [ ] Stored procedures created (07_Create_Procedures.sql)
- [ ] Dim_Date populated (2923 rows)
- [ ] Static dimensions populated (TingkatKeparahan, JenisInsiden, JenisLimbah)
- [ ] Airflow initialized
- [ ] Airflow admin user created
- [ ] Airflow connection to SQL Server configured
- [ ] Sample DAG created
- [ ] Airflow webserver running (port 8080)
- [ ] Airflow scheduler running
- [ ] Database connection test passed
- [ ] Stored procedure test passed
- [ ] Data quality check executed
- [ ] Documentation reviewed

---

## Next Steps

Setelah instalasi selesai, lanjutkan ke:
1. [DEPLOYMENT.md](DEPLOYMENT.md) - Panduan deployment ke production
2. [etl-documentation.md](etl-documentation.md) - Detail proses ETL
3. [README.md](../README.md) - Overview project

---

**Document Version:** 1.0  
**Last Updated:** 2024  
**Maintainer:** K3L Team ITERA
