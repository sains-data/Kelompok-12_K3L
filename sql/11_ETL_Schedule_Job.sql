USE msdb;
GO

-------------------------------------------------------
-- 1. CREATE JOB jika belum ada
-------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'ETL_K3L_Daily_Load')
BEGIN
    EXEC sp_add_job 
        @job_name = N'ETL_K3L_Daily_Load',
        @enabled = 1,
        @description = N'Daily ETL load for K3L Data Mart';
END
GO

-------------------------------------------------------
-- 2. CREATE / UPDATE JOB STEP
-------------------------------------------------------
-- Hapus step jika sebelumnya sudah ada (agar tidak duplikat)
IF EXISTS (
    SELECT 1 FROM msdb.dbo.sysjobsteps 
    WHERE step_name = 'Execute Master ETL'
      AND job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = 'ETL_K3L_Daily_Load')
)
BEGIN
    EXEC msdb.dbo.sp_delete_jobstep 
      @job_name = 'ETL_K3L_Daily_Load',
      @step_name = 'Execute Master ETL';
END
GO

EXEC sp_add_jobstep 
    @job_name = N'ETL_K3L_Daily_Load',
    @step_name = N'Execute Master ETL',
    @subsystem = N'TSQL',
    @command = N'EXEC dbo.usp_Master_ETL;',   -- GANTI jika SP Anda berbeda
    @database_name = N'K3L_DataMart',         -- DB ANDA
    @retry_attempts = 3,
    @retry_interval = 5;
GO

-------------------------------------------------------
-- 3. CREATE SCHEDULE jika belum ada
-------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = 'Daily at 2 AM')
BEGIN
    EXEC sp_add_schedule 
        @schedule_name = N'Daily at 2 AM',
        @freq_type = 4,         -- Daily
        @freq_interval = 1,     -- Setiap hari
        @active_start_time = 020000;  -- 02:00 AM
END
GO

-------------------------------------------------------
-- 4. ATTACH SCHEDULE ke JOB
-------------------------------------------------------
BEGIN TRY
    EXEC sp_attach_schedule 
        @job_name = N'ETL_K3L_Daily_Load',
        @schedule_name = N'Daily at 2 AM';
END TRY
BEGIN CATCH
    -- Jika sudah pernah ter-attach, abaikan error
END CATCH
GO

-------------------------------------------------------
-- 5. Registrasikan ke SQL Server Agent
-------------------------------------------------------
EXEC sp_add_jobserver 
    @job_name = N'ETL_K3L_Daily_Load';
GO
