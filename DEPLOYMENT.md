# Panduan Deployment K3L Data Mart

## Daftar Isi
1. [Pre-Deployment](#pre-deployment)
2. [Deployment Checklist](#deployment-checklist)
3. [Configuration](#configuration)
4. [Deployment Steps](#deployment-steps)
5. [Verification](#verification)
6. [Post-Deployment](#post-deployment)
7. [Rollback Plan](#rollback-plan)

---

## 1. Pre-Deployment

### 1.1 Prerequisites

Sebelum melakukan deployment, pastikan:

- [ ] Semua step di [INSTALLATION.md](INSTALLATION.md) sudah selesai
- [ ] Testing di development environment sukses
- [ ] Backup database existing (jika ada)
- [ ] Dokumentasi sudah diupdate
- [ ] Akses production server sudah diberikan
- [ ] Approval dari stakeholder sudah didapat

### 1.2 Backup Strategy

```bash
# Backup database existing (jika ada)
export BACKUP_DATE=$(date +%Y%m%d_%H%M%S)
sqlcmd -d K3L_DataMart <<EOF
BACKUP DATABASE K3L_DataMart 
TO DISK = '/var/opt/mssql/data/backups/K3L_DataMart_${BACKUP_DATE}.bak'
WITH FORMAT, COMPRESSION;
GO
EOF

# Backup configuration files
mkdir -p ~/k3l_backups/${BACKUP_DATE}
cp -r ~/k3l ~/k3l_backups/${BACKUP_DATE}/
cp -r ~/airflow/dags ~/k3l_backups/${BACKUP_DATE}/
```

### 1.3 Maintenance Window

**Recommended maintenance window:**
- **Waktu**: Sabtu/Minggu, 22:00 - 06:00 WIB
- **Durasi**: 4-6 jam
- **Rollback window**: 2 jam

**Komunikasi:**
```
Subject: [K3L Data Mart] Scheduled Maintenance

Dear Users,

K3L Data Mart akan menjalani maintenance pada:
- Tanggal: [DATE]
- Waktu: 22:00 - 06:00 WIB
- Downtime: 4-6 jam

Selama maintenance:
- Dashboard K3L tidak dapat diakses
- ETL jobs akan dijadwalkan ulang
- Data akan kembali tersedia setelah maintenance selesai

Contact: k3l@itera.ac.id

Terima kasih,
K3L Team
```

---

## 2. Deployment Checklist

### 2.1 Infrastructure Checklist

- [ ] Server production ready (CPU, RAM, Disk)
- [ ] SQL Server 2022 installed dan configured
- [ ] Firewall rules configured (port 1433, 8080)
- [ ] Disk space sufficient (minimal 50GB free)
- [ ] Network connectivity verified
- [ ] Monitoring tools installed (optional: Prometheus, Grafana)

### 2.2 Database Checklist

- [ ] SQL Server service running
- [ ] SQL Server Agent enabled
- [ ] SA password set dan didokumentasikan secara aman
- [ ] Database backup policy configured
- [ ] Transaction log backup configured
- [ ] Recovery model = FULL
- [ ] Max memory configured (70-80% of total RAM)

### 2.3 Application Checklist

- [ ] Python 3.8+ installed
- [ ] Virtual environment created
- [ ] All Python dependencies installed
- [ ] Airflow initialized
- [ ] Airflow admin user created
- [ ] Airflow systemd services created
- [ ] Connection strings configured
- [ ] Environment variables set

### 2.4 Security Checklist

- [ ] SA password stored in secrets manager (atau .env dengan chmod 600)
- [ ] ETL user created dengan least privilege
- [ ] Firewall configured (allow only necessary ports)
- [ ] SSL/TLS enabled untuk SQL Server (optional tapi recommended)
- [ ] Airflow web UI password changed dari default
- [ ] File permissions set correctly (chmod 600 untuk .env)

---

## 3. Configuration

### 3.1 Production Environment Variables

```bash
# Create production .env
cat > ~/k3l/.env.prod <<EOF
# Database
DB_SERVER=<PROD_SERVER_IP>
DB_NAME=K3L_DataMart
DB_USER=k3l_etl_user
DB_PASSWORD=<SECURE_PASSWORD>
DB_PORT=1433

# Airflow
AIRFLOW_HOME=/home/k3l_user/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/k3l_user/airflow/airflow.db

# Data paths
DATA_SOURCE_PATH=/data/k3l/sources
LOG_PATH=/var/log/k3l

# Email notifications (optional)
SMTP_HOST=smtp.itera.ac.id
SMTP_PORT=587
SMTP_USER=k3l@itera.ac.id
SMTP_PASSWORD=<EMAIL_PASSWORD>
EMAIL_RECIPIENTS=k3l-team@itera.ac.id
EOF

# Secure .env file
chmod 600 ~/k3l/.env.prod
```

### 3.2 SQL Server Configuration (Production)

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Set recovery model to FULL
ALTER DATABASE K3L_DataMart SET RECOVERY FULL;

-- Configure max memory (80% of 8GB = 6400MB)
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'max server memory (MB)', 6400;
RECONFIGURE;

-- Enable backup compression
EXEC sp_configure 'backup compression default', 1;
RECONFIGURE;

-- Configure MAXDOP (untuk 4 cores = 4)
EXEC sp_configure 'max degree of parallelism', 4;
RECONFIGURE;
GO

-- Create maintenance plan user
CREATE LOGIN k3l_maint_user WITH PASSWORD = '<MAINT_PASSWORD>';
CREATE USER k3l_maint_user FOR LOGIN k3l_maint_user;
ALTER SERVER ROLE sysadmin ADD MEMBER k3l_maint_user;
GO
EOF
```

### 3.3 Airflow Production Configuration

```bash
# Edit airflow.cfg untuk production
vim ~/airflow/airflow.cfg
```

**Key settings untuk production:**
```ini
[core]
dags_folder = /home/k3l_user/k3l/etl/dags
executor = LocalExecutor  # atau CeleryExecutor untuk distributed
load_examples = False
max_active_runs_per_dag = 1
parallelism = 8

[scheduler]
catchup_by_default = False
max_threads = 4
scheduler_heartbeat_sec = 5

[webserver]
web_server_port = 8080
web_server_host = 0.0.0.0
expose_config = False  # Security: hide config in UI
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth

[logging]
base_log_folder = /var/log/k3l/airflow
remote_logging = False
logging_level = INFO

[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = smtp.itera.ac.id
smtp_starttls = True
smtp_ssl = False
smtp_user = k3l@itera.ac.id
smtp_password = <EMAIL_PASSWORD>
smtp_port = 587
smtp_mail_from = k3l@itera.ac.id
```

---

## 4. Deployment Steps

### 4.1 Step 1: Deploy Database Objects

```bash
# Navigate to SQL scripts
cd ~/k3l/sql

# Set environment
export SQLCMDSERVER=<PROD_SERVER>
export SQLCMDUSER=sa
export SQLCMDPASSWORD='<SA_PASSWORD>'

# Execute in order (dengan logging)
echo "Starting database deployment at $(date)" | tee /tmp/deployment.log

echo "Step 1: Create Database..." | tee -a /tmp/deployment.log
sqlcmd -i 01_Create_Database.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 2: Create Dimensions..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 02_Create_Dimensions.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 3: Create Facts..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 03_Create_Facts.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 4: Create Partitions..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 05_Create_Partitions.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 5: Create Indexes..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 04_Create_Indexes.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 6: Create Staging..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 06_Create_Staging.sql 2>&1 | tee -a /tmp/deployment.log

echo "Step 7: Create Procedures..." | tee -a /tmp/deployment.log
sqlcmd -d K3L_DataMart -i 07_Create_Procedures.sql 2>&1 | tee -a /tmp/deployment.log

echo "Database deployment completed at $(date)" | tee -a /tmp/deployment.log

# Check for errors
if grep -qi "error" /tmp/deployment.log; then
    echo "ERRORS FOUND! Check /tmp/deployment.log"
    exit 1
else
    echo "SUCCESS! No errors found."
fi
```

### 4.2 Step 2: Populate Master Data

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Dim_Date (2023-2030)
DECLARE @StartDate DATE = '2023-01-01';
DECLARE @EndDate DATE = '2030-12-31';
WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dbo.Dim_Date (
        DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
        WeekOfYear, MonthNumber, MonthName, Quarter, QuarterName, Year, IsWeekend
    )
    VALUES (
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT), @StartDate,
        DATEPART(WEEKDAY, @StartDate), DATENAME(WEEKDAY, @StartDate),
        DATEPART(DAY, @StartDate), DATEPART(DAYOFYEAR, @StartDate),
        DATEPART(WEEK, @StartDate), DATEPART(MONTH, @StartDate),
        DATENAME(MONTH, @StartDate), DATEPART(QUARTER, @StartDate),
        'Q' + CAST(DATEPART(QUARTER, @StartDate) AS VARCHAR) + '-' + CAST(YEAR(@StartDate) AS VARCHAR),
        YEAR(@StartDate),
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
    );
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END

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

-- Verification
SELECT 'Dim_Date' AS TableName, COUNT(*) AS RowCount FROM Dim_Date
UNION ALL
SELECT 'Dim_TingkatKeparahan', COUNT(*) FROM Dim_TingkatKeparahan
UNION ALL
SELECT 'Dim_JenisInsiden', COUNT(*) FROM Dim_JenisInsiden
UNION ALL
SELECT 'Dim_JenisLimbah', COUNT(*) FROM Dim_JenisLimbah;
GO
EOF
```

### 4.3 Step 3: Deploy Airflow DAGs

```bash
# Copy DAGs to production
cp -r ~/k3l/etl/dags/* ~/airflow/dags/

# Check DAG syntax
cd ~/airflow/dags
for dag in *.py; do
    echo "Checking $dag..."
    python $dag
    if [ $? -ne 0 ]; then
        echo "ERROR in $dag"
        exit 1
    fi
done

# Restart Airflow services
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler

# Wait for services to start
sleep 10

# Check services status
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler
```

### 4.4 Step 4: Configure Airflow Connections

```bash
# Add production connection
airflow connections add k3l_db_prod \
    --conn-type mssql \
    --conn-host <PROD_SERVER_IP> \
    --conn-schema K3L_DataMart \
    --conn-login k3l_etl_user \
    --conn-password '<ETL_USER_PASSWORD>' \
    --conn-port 1433

# Test connection
airflow connections test k3l_db_prod
```

---

## 5. Verification

### 5.1 Database Verification

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Check tables
SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    p.rows AS RowCount,
    SUM(a.total_pages) * 8 AS TotalSpaceKB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.name IN (
    'Dim_Date', 'Dim_Lokasi', 'Dim_UnitKerja', 'Dim_JenisInsiden',
    'Dim_TingkatKeparahan', 'Dim_Peralatan', 'Dim_JenisLimbah',
    'Fact_Insiden', 'Fact_Inspeksi', 'Fact_Limbah',
    'STG_Lokasi', 'STG_UnitKerja', 'STG_Peralatan',
    'STG_Insiden', 'STG_Inspeksi', 'STG_Limbah'
)
GROUP BY t.schema_id, t.name, p.rows
ORDER BY SchemaName, TableName;

-- Check indexes
SELECT 
    COUNT(*) AS IndexCount
FROM sys.indexes
WHERE name LIKE 'IX_%' OR name LIKE 'UIX_%';

-- Check procedures
SELECT name, create_date
FROM sys.procedures
WHERE name LIKE 'usp_%'
ORDER BY name;

-- Check partitions
SELECT 
    p.partition_number,
    f.name AS FileGroupName,
    p.rows AS RowCount
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE p.object_id = OBJECT_ID('dbo.Fact_Insiden')
    AND p.index_id IN (0, 1)
ORDER BY p.partition_number;
GO
EOF
```

**Expected results:**
- 7 dimension tables
- 3 fact tables
- 6 staging tables
- ~35 indexes
- 11 stored procedures
- 33 partitions (2023 Q1 - 2030 Q4)

### 5.2 Airflow Verification

```bash
# List DAGs
airflow dags list

# Check DAG bag import errors
airflow dags list-import-errors

# Test DAG (dry run)
airflow dags test k3l_etl_master 2024-01-01

# Check scheduler health
airflow dags list-jobs --dag-id k3l_etl_master --state running
```

### 5.3 End-to-End Test

```bash
# 1. Insert sample data ke staging
sqlcmd -d K3L_DataMart <<EOF
-- Sample lokasi
INSERT INTO STG_Lokasi (NamaLokasi, TipeLokasi, Gedung, Lantai, Kapasitas)
VALUES ('Test Lab', 'Laboratorium', 'GKU', '3', '30');

-- Sample insiden
INSERT INTO STG_Insiden (
    LokasiNama, UnitKerjaNama, JenisInsidenNama, TingkatKeparahanNama,
    TanggalInsiden, WaktuInsiden, Deskripsi, KorbanJiwa, StatusPenanganan
)
VALUES (
    'Test Lab', NULL, 'Kebakaran', 'Ringan',
    '2024-01-15', '10:30:00', 'Test insiden', '0', 'Selesai'
);
GO
EOF

# 2. Run ETL manually
airflow dags trigger k3l_etl_master

# 3. Wait for completion (check UI or CLI)
sleep 60

# 4. Verify data loaded
sqlcmd -d K3L_DataMart <<EOF
SELECT * FROM Dim_Lokasi WHERE NamaLokasi = 'Test Lab';
SELECT * FROM Fact_Insiden WHERE Deskripsi = 'Test insiden';
SELECT * FROM STG_Insiden WHERE Deskripsi = 'Test insiden';
GO
EOF

# 5. Cleanup test data
sqlcmd -d K3L_DataMart <<EOF
DELETE FROM Fact_Insiden WHERE Deskripsi = 'Test insiden';
DELETE FROM Dim_Lokasi WHERE NamaLokasi = 'Test Lab';
DELETE FROM STG_Insiden WHERE Deskripsi = 'Test insiden';
DELETE FROM STG_Lokasi WHERE NamaLokasi = 'Test Lab';
GO
EOF
```

### 5.4 Performance Test

```bash
sqlcmd -d K3L_DataMart <<EOF
-- Test query performance
SET STATISTICS TIME ON;
SET STATISTICS IO ON;

-- Query 1: Insiden per bulan
SELECT 
    d.Year, d.MonthNumber, d.MonthName,
    COUNT(*) AS TotalInsiden,
    SUM(f.KorbanJiwa) AS TotalKorbanJiwa
FROM Fact_Insiden f
INNER JOIN Dim_Date d ON f.DateKey = d.DateKey
WHERE d.Year = 2024
GROUP BY d.Year, d.MonthNumber, d.MonthName
ORDER BY d.Year, d.MonthNumber;

-- Query 2: Inspeksi per lokasi
SELECT 
    l.NamaLokasi, l.Gedung,
    COUNT(*) AS TotalInspeksi,
    SUM(CASE WHEN f.HasilInspeksi = 'Lolos' THEN 1 ELSE 0 END) AS JumlahLolos
FROM Fact_Inspeksi f
INNER JOIN Dim_Lokasi l ON f.LokasiKey = l.LokasiKey
GROUP BY l.NamaLokasi, l.Gedung
ORDER BY TotalInspeksi DESC;

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
GO
EOF
```

**Performance targets:**
- Queries < 5 seconds
- Logical reads < 10000 pages
- ETL runtime < 1 hour (untuk 1 tahun data)

---

## 6. Post-Deployment

### 6.1 Enable Monitoring

```bash
# Setup log rotation
sudo tee /etc/logrotate.d/k3l <<EOF
/var/log/k3l/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 0640 k3l_user k3l_user
    sharedscripts
    postrotate
        systemctl reload airflow-webserver > /dev/null 2>&1 || true
        systemctl reload airflow-scheduler > /dev/null 2>&1 || true
    endscript
}
EOF

# Test log rotation
sudo logrotate -f /etc/logrotate.d/k3l
```

### 6.2 Setup Automated Backups

```bash
# Create backup script
cat > ~/k3l/scripts/backup_k3l.sh <<'EOF'
#!/bin/bash
BACKUP_DIR=/var/opt/mssql/data/backups
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=30

# Full backup
sqlcmd -d K3L_DataMart <<SQL
BACKUP DATABASE K3L_DataMart 
TO DISK = '${BACKUP_DIR}/K3L_DataMart_${TIMESTAMP}.bak'
WITH FORMAT, COMPRESSION, STATS = 10;
GO
SQL

# Delete old backups
find ${BACKUP_DIR} -name "K3L_DataMart_*.bak" -mtime +${RETENTION_DAYS} -delete

echo "Backup completed: K3L_DataMart_${TIMESTAMP}.bak"
EOF

chmod +x ~/k3l/scripts/backup_k3l.sh

# Add to crontab (daily at 1 AM)
(crontab -l 2>/dev/null; echo "0 1 * * * ~/k3l/scripts/backup_k3l.sh >> /var/log/k3l/backup.log 2>&1") | crontab -
```

### 6.3 Configure Alerts

```python
# Add to Airflow DAG (email on failure)
def send_failure_alert(context):
    from airflow.utils.email import send_email
    subject = f"K3L ETL Failed: {context['task_instance'].task_id}"
    html_content = f"""
    <h3>ETL Task Failed</h3>
    <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
    <p><strong>Task:</strong> {context['task_instance'].task_id}</p>
    <p><strong>Execution Date:</strong> {context['execution_date']}</p>
    <p><strong>Log URL:</strong> {context['task_instance'].log_url}</p>
    <p><strong>Error:</strong> {context.get('exception', 'N/A')}</p>
    """
    send_email(
        to=['k3l-team@itera.ac.id'],
        subject=subject,
        html_content=html_content
    )

# In DAG definition
default_args = {
    ...
    'on_failure_callback': send_failure_alert
}
```

### 6.4 Document Go-Live

```bash
# Create deployment report
cat > /tmp/deployment_report.txt <<EOF
K3L DATA MART - DEPLOYMENT REPORT
==================================

Deployment Date: $(date)
Deployed By: $(whoami)
Server: $(hostname)

Database:
- Name: K3L_DataMart
- Size: $(sqlcmd -d K3L_DataMart -Q "EXEC sp_spaceused" -h-1 | tail -1)
- Tables: $(sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) FROM sys.tables" -h-1)
- Procedures: $(sqlcmd -d K3L_DataMart -Q "SELECT COUNT(*) FROM sys.procedures" -h-1)

Airflow:
- Version: $(airflow version)
- DAGs: $(airflow dags list | wc -l)
- Webserver: http://$(hostname):8080

Status: SUCCESS

Next Steps:
1. Monitor ETL runs for first week
2. Collect user feedback
3. Schedule knowledge transfer session
4. Review and optimize performance
EOF

# Email report
mail -s "K3L Data Mart Deployment Report" k3l-team@itera.ac.id < /tmp/deployment_report.txt
```

---

## 7. Rollback Plan

### 7.1 Rollback Triggers

Lakukan rollback jika:
- [ ] Database deployment failure
- [ ] ETL errors > 50%
- [ ] Performance degradation > 200%
- [ ] Data corruption detected
- [ ] Critical bug in production

### 7.2 Rollback Steps

```bash
# 1. Stop Airflow services
sudo systemctl stop airflow-webserver
sudo systemctl stop airflow-scheduler

# 2. Restore database from backup
export BACKUP_FILE="<backup_filename>.bak"
sqlcmd <<EOF
USE master;
GO
ALTER DATABASE K3L_DataMart SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
RESTORE DATABASE K3L_DataMart 
FROM DISK = '/var/opt/mssql/data/backups/${BACKUP_FILE}'
WITH REPLACE, RECOVERY;
GO
ALTER DATABASE K3L_DataMart SET MULTI_USER;
GO
EOF

# 3. Restore old DAGs
rm -rf ~/airflow/dags/*
cp -r ~/k3l_backups/<backup_date>/dags/* ~/airflow/dags/

# 4. Restart services
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler

# 5. Verify rollback
sqlcmd -d K3L_DataMart -Q "SELECT * FROM sys.tables"
airflow dags list

# 6. Notify stakeholders
echo "Rollback completed at $(date)" | mail -s "K3L Rollback Notification" k3l-team@itera.ac.id
```

### 7.3 Post-Rollback

- [ ] Root cause analysis meeting
- [ ] Fix issues in development
- [ ] Re-test thoroughly
- [ ] Schedule new deployment

---

## Deployment Sign-Off

| Role | Name | Signature | Date |
|------|------|-----------|------|
| **Project Manager** | | | |
| **Database Admin** | | | |
| **Data Engineer** | | | |
| **QA Lead** | | | |
| **Business Owner** | | | |

---

## Support Contacts

| Issue Type | Contact | Email | Phone |
|------------|---------|-------|-------|
| **Database** | IT ITERA | it@itera.ac.id | - |
| **ETL/Airflow** | K3L Team | k3l@itera.ac.id | - |
| **Business Logic** | Unit K3L | k3l-unit@itera.ac.id | - |
| **Emergency** | On-call DBA | - | - |

---

**Document Version:** 1.0  
**Last Updated:** 2024  
**Next Review:** After first deployment
