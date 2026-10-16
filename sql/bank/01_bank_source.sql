/* =====================================================================
   PROJECT 1 - BANKING : Azure SQL part + metadata for ALL THREE sources
     customers    -> Azure SQL DB  (this database)     source_type = AZURE_SQL
     accounts     -> Azure SQL DB  (this database)     source_type = AZURE_SQL
     transactions -> on-prem SQL Server (laptop, SHIR) source_type = ONPREM_SQL
   Run in Azure SQL (sqldb-finserve-rohit) AFTER 00_control_framework.sql
   ===================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bank') EXEC('CREATE SCHEMA bank');
GO

DROP TABLE IF EXISTS bank.customers;
CREATE TABLE bank.customers (
    CustomerID    INT,
    FirstName     NVARCHAR(100),
    LastName      NVARCHAR(100),
    Email         NVARCHAR(200),
    Phone         NVARCHAR(50),
    City          NVARCHAR(100),
    State         NVARCHAR(100),
    CreatedDate   DATETIME2(3),
    ModifiedDate  DATETIME2(3),
    Status        NVARCHAR(20)
);
CREATE NONCLUSTERED INDEX ix_customers_modified ON bank.customers(ModifiedDate);

DROP TABLE IF EXISTS bank.accounts;
CREATE TABLE bank.accounts (
    AccountID     INT,
    CustomerID    INT,
    AccountNumber VARCHAR(30),       -- arrives as '636800771214.0' (float artefact) -> fixed in Silver
    AccountType   NVARCHAR(30),
    OpenDate      DATE,
    Balance       DECIMAL(18,2),
    Status        NVARCHAR(20),
    ModifiedDate  DATETIME2(3)       -- DATETIME2 (not DATE) so same-day changes are not missed by the watermark
);
CREATE NONCLUSTERED INDEX ix_accounts_modified ON bank.accounts(ModifiedDate);
GO

/* ---------------- metadata: one row per source table ---------------- */
DELETE FROM ctl.ingestion_config WHERE project = 'bank';
INSERT INTO ctl.ingestion_config (project, source_type, source_schema, source_table, primary_key, load_type, watermark_column, target_container, target_folder, load_priority)
VALUES ('bank','AZURE_SQL', 'bank',   'customers',   'CustomerID',   'INCREMENTAL','ModifiedDate','bronzefinserve','bank/customers',   1),
       ('bank','AZURE_SQL', 'bank',   'accounts',    'AccountID',    'INCREMENTAL','ModifiedDate','bronzefinserve','bank/accounts',    1),
       ('bank','ONPREM_SQL','bank',   'transactions','TransactionID','INCREMENTAL','ModifiedDate','bronzefinserve','bank/transactions',2);

/* customers + accounts are seeded from CSVs into Azure SQL (transactions live on-prem) */
DELETE FROM ctl.seed_config WHERE project = 'bank';
INSERT INTO ctl.seed_config (project, landing_folder, file_name, target_schema, target_table)
VALUES ('bank','bank','customers_raw.csv','bank','customers'),
       ('bank','bank','accounts_raw.csv','bank','accounts');
GO

/* ---------------------------------------------------------------------
   One "business day" of account changes
   EXEC bank.usp_simulate_daily_accounts;
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE bank.usp_simulate_daily_accounts
    @new_accounts INT = 800, @upd_accounts INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @max_acct INT = (SELECT MAX(AccountID) FROM bank.accounts);

    ;WITH n AS (SELECT TOP (@new_accounts) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b)
    INSERT INTO bank.accounts (AccountID, CustomerID, AccountNumber, AccountType, OpenDate, Balance, Status, ModifiedDate)
    SELECT @max_acct + i,
           CASE WHEN i % 50 = 0 THEN 900000 + i                       -- customer not (yet) in CRM -> late-arriving
                ELSE 1 + ABS(CHECKSUM(NEWID())) % 100000 END,
           CONCAT(CAST(100000000000 + ABS(CHECKSUM(NEWID())) % 899999999999 AS VARCHAR(20)), '.0'),
           CHOOSE(i % 3 + 1, 'Savings','Current','Fixed Deposit'),
           CAST(@now AS DATE),
           CAST(ABS(CHECKSUM(NEWID())) % 50000000 / 100.0 AS DECIMAL(18,2)),
           'Active', @now
    FROM n;

    UPDATE a SET Balance = a.Balance + (ABS(CHECKSUM(NEWID())) % 2000000 / 100.0) - 10000,
                 Status  = CASE WHEN ABS(CHECKSUM(NEWID())) % 20 = 0 THEN 'CLOSED' ELSE a.Status END,
                 ModifiedDate = @now
    FROM bank.accounts a
    WHERE a.AccountID IN (SELECT TOP (@upd_accounts) AccountID FROM bank.accounts WHERE AccountID <= @max_acct ORDER BY NEWID());

    SELECT COUNT(*) AS changed_rows FROM bank.accounts WHERE ModifiedDate = @now;
END
GO
