/* =====================================================================
   00_control_framework.sql
   Shared metadata + audit framework used by ALL three projects.
   Run once in Azure SQL DB (sqldb-finserve-rohit) via Query editor.
   ===================================================================== */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl') EXEC('CREATE SCHEMA ctl');
GO

/* ---------------------------------------------------------------------
   1. Ingestion config  (one row = one source table to ingest)
      The ADF master pipeline does a Lookup on this table and loops
      (ForEach) over the rows -> metadata-driven ingestion.
   --------------------------------------------------------------------- */
IF OBJECT_ID('ctl.ingestion_config') IS NULL
CREATE TABLE ctl.ingestion_config (
    config_id         INT IDENTITY(1,1) PRIMARY KEY,
    project           VARCHAR(50)   NOT NULL,          -- bank | ecom | scm
    source_type       VARCHAR(20)   NOT NULL DEFAULT 'AZURE_SQL',   -- AZURE_SQL | ONPREM_SQL | REST_API
    source_schema     VARCHAR(50)   NOT NULL,                       -- schema (SQL) or API name
    source_table      VARCHAR(100)  NOT NULL,                       -- table (SQL) or API resource e.g. customers
    primary_key       VARCHAR(200)  NOT NULL,          -- used later by Databricks MERGE
    load_type         VARCHAR(20)   NOT NULL DEFAULT 'INCREMENTAL', -- INCREMENTAL | FULL
    watermark_column  VARCHAR(100)  NULL,              -- NULL for FULL loads
    last_watermark    DATETIME2(3)  NOT NULL DEFAULT '1900-01-01',
    target_container  VARCHAR(50)   NOT NULL DEFAULT 'bronzefinserve',
    target_folder     VARCHAR(200)  NOT NULL,          -- e.g. bank/customers
    load_priority     INT           NOT NULL DEFAULT 1,
    is_active         BIT           NOT NULL DEFAULT 1,
    updated_at        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_ingestion_config UNIQUE (project, source_schema, source_table)
);
GO
-- if you created the table before source_type existed:
IF COL_LENGTH('ctl.ingestion_config', 'source_type') IS NULL
    ALTER TABLE ctl.ingestion_config ADD source_type VARCHAR(20) NOT NULL DEFAULT 'AZURE_SQL';
GO

/* ---------------------------------------------------------------------
   2. Run log / audit  (one row per table per pipeline run)
   --------------------------------------------------------------------- */
IF OBJECT_ID('ctl.pipeline_run_log') IS NULL
CREATE TABLE ctl.pipeline_run_log (
    log_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id   VARCHAR(100)  NOT NULL,
    parent_run_id     VARCHAR(100)  NULL,
    pipeline_name     VARCHAR(200)  NOT NULL,
    project           VARCHAR(50)   NOT NULL,
    source_table      VARCHAR(200)  NOT NULL,
    status            VARCHAR(20)   NOT NULL,          -- RUNNING | SUCCEEDED | NO_NEW_DATA | FAILED
    rows_read         BIGINT        NULL,
    rows_copied       BIGINT        NULL,
    old_watermark     VARCHAR(30)   NULL,
    new_watermark     VARCHAR(30)   NULL,
    file_path         VARCHAR(500)  NULL,
    error_message     NVARCHAR(4000) NULL,
    start_time        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    end_time          DATETIME2(3)  NULL
);
GO

/* ---------------------------------------------------------------------
   3. Seed config  (one-time: which CSV in landing goes to which SQL table)
   --------------------------------------------------------------------- */
IF OBJECT_ID('ctl.seed_config') IS NULL
CREATE TABLE ctl.seed_config (
    seed_id        INT IDENTITY(1,1) PRIMARY KEY,
    project        VARCHAR(50)  NOT NULL,
    landing_folder VARCHAR(200) NOT NULL,   -- folder inside 'landing' container
    file_name      VARCHAR(200) NOT NULL,
    target_schema  VARCHAR(50)  NOT NULL,
    target_table   VARCHAR(100) NOT NULL
);
GO

/* =====================================================================
   Stored procedures called by ADF "Stored procedure" activities
   ===================================================================== */

CREATE OR ALTER PROCEDURE ctl.usp_get_active_tables
    @project VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT  config_id, project, source_type, source_schema, source_table, primary_key, load_type,
            ISNULL(watermark_column, '')                    AS watermark_column,
            CONVERT(VARCHAR(23), last_watermark, 121)       AS last_watermark,   -- 'yyyy-mm-dd hh:mi:ss.mmm'
            target_container, target_folder, load_priority
    FROM    ctl.ingestion_config
    WHERE   project = @project AND is_active = 1
    ORDER BY load_priority, source_table;
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_log_run_start
    @pipeline_run_id VARCHAR(100), @parent_run_id VARCHAR(100), @pipeline_name VARCHAR(200),
    @project VARCHAR(50), @source_table VARCHAR(200), @old_watermark VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.pipeline_run_log (pipeline_run_id, parent_run_id, pipeline_name, project, source_table, status, old_watermark)
    VALUES (@pipeline_run_id, @parent_run_id, @pipeline_name, @project, @source_table, 'RUNNING', @old_watermark);
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_log_run_end
    @pipeline_run_id VARCHAR(100), @source_table VARCHAR(200), @status VARCHAR(20),
    @rows_read BIGINT = NULL, @rows_copied BIGINT = NULL, @new_watermark VARCHAR(30) = NULL,
    @file_path VARCHAR(500) = NULL, @error_message NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ctl.pipeline_run_log
       SET status = @status, rows_read = @rows_read, rows_copied = @rows_copied,
           new_watermark = @new_watermark, file_path = @file_path,
           error_message = LEFT(@error_message, 4000), end_time = SYSUTCDATETIME()
     WHERE pipeline_run_id = @pipeline_run_id AND source_table = @source_table;
END
GO

/* Watermark only moves AFTER the copy succeeded -> a failed run simply
   re-reads the same window next time (restartable, no data loss).      */
CREATE OR ALTER PROCEDURE ctl.usp_update_watermark
    @config_id INT, @new_watermark VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ctl.ingestion_config
       SET last_watermark = CONVERT(DATETIME2(3), @new_watermark, 121),
           updated_at     = SYSUTCDATETIME()
     WHERE config_id = @config_id;
END
GO

/* Back-dated refresh: reset a table's watermark to reload history */
CREATE OR ALTER PROCEDURE ctl.usp_reset_watermark
    @project VARCHAR(50), @source_table VARCHAR(100), @reset_to DATETIME2(3) = '1900-01-01'
AS
BEGIN
    UPDATE ctl.ingestion_config SET last_watermark = @reset_to, updated_at = SYSUTCDATETIME()
     WHERE project = @project AND source_table = @source_table;
END
GO

/* Handy monitoring view */
CREATE OR ALTER VIEW ctl.vw_latest_runs AS
SELECT TOP 200 project, source_table, status, rows_read, rows_copied,
       old_watermark, new_watermark, DATEDIFF(SECOND, start_time, end_time) AS duration_sec,
       start_time, error_message, pipeline_run_id
FROM ctl.pipeline_run_log
ORDER BY log_id DESC;
GO
