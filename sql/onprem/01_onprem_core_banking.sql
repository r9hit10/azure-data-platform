/* =====================================================================
   ON-PREM "CORE BANKING" DATABASE  (SQL Server Express on your laptop)
   Run in SSMS connected to  localhost\SQLEXPRESS  (Windows authentication).
   Before running: copy transactions_raw.csv to  C:\finserve\transactions_raw.csv
   ===================================================================== */
IF DB_ID('CoreBankingDB') IS NULL CREATE DATABASE CoreBankingDB;
GO
USE CoreBankingDB;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bank') EXEC('CREATE SCHEMA bank');
GO
DROP TABLE IF EXISTS bank.transactions;
CREATE TABLE bank.transactions (
    TransactionID   BIGINT,
    AccountID       INT,
    TransactionType NVARCHAR(10),
    Amount          DECIMAL(18,2),
    TransactionDate DATETIME2(0),
    Status          NVARCHAR(20),
    CreatedDate     DATETIME2(0),
    ModifiedDate    DATETIME2(0)
);
GO

/* Load the 2,020,000 rows (about 30-60 seconds). Empty TransactionDate values become NULL. */
BULK INSERT bank.transactions
FROM 'C:\finserve\transactions_raw.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', KEEPNULLS, TABLOCK);
GO
CREATE NONCLUSTERED INDEX ix_transactions_modified ON bank.transactions(ModifiedDate);
GO
SELECT COUNT(*) AS rows_loaded FROM bank.transactions;    -- expect 2,020,000
GO

/* ---------------------------------------------------------------------
   One "business day" of new / changed transactions (run before an incremental test)
   EXEC bank.usp_simulate_daily_transactions;
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE bank.usp_simulate_daily_transactions
    @new_txns INT = 20000, @upd_txns INT = 500
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME2(0) = SYSUTCDATETIME();
    DECLARE @max_txn BIGINT = (SELECT MAX(TransactionID) FROM bank.transactions);

    ;WITH n AS (SELECT TOP (@new_txns) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b)
    INSERT INTO bank.transactions (TransactionID, AccountID, TransactionType, Amount, TransactionDate, Status, CreatedDate, ModifiedDate)
    SELECT @max_txn + i,
           1 + ABS(CHECKSUM(NEWID())) % 150000,
           CASE WHEN i % 5 < 3 THEN 'Debit' ELSE 'Credit' END,
           CASE WHEN i % 400 = 0 THEN 0 ELSE CAST(ABS(CHECKSUM(NEWID())) % 10000000 / 100.0 AS DECIMAL(18,2)) END,
           CASE WHEN i % 150 = 0 THEN NULL ELSE DATEADD(SECOND, -(i % 86400), @now) END,
           CASE WHEN i % 20 = 0 THEN 'Pending' WHEN i % 30 = 0 THEN 'FAILED' ELSE 'Success' END,
           DATEADD(SECOND, -(i % 86400), @now), @now
    FROM n;

    -- ~1% duplicates (source system re-sends)
    INSERT INTO bank.transactions
    SELECT TOP (@new_txns / 100) * FROM bank.transactions WHERE TransactionID > @max_txn ORDER BY NEWID();

    -- pending -> success on older transactions
    UPDATE t SET Status = 'Success', ModifiedDate = @now
    FROM bank.transactions t
    WHERE t.TransactionID IN (SELECT TOP (@upd_txns) TransactionID FROM bank.transactions
                              WHERE Status IN ('Pending','PENDING') AND TransactionID <= @max_txn ORDER BY NEWID());

    SELECT COUNT(*) AS changed_rows FROM bank.transactions WHERE ModifiedDate = @now;
END
GO

/* ---------------------------------------------------------------------
   Read-only SQL login that ADF uses through the Self-hosted Integration Runtime.
   Requires "SQL Server and Windows Authentication mode" (see build steps) + service restart.
   --------------------------------------------------------------------- */
USE master;
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'adf_reader')
    CREATE LOGIN adf_reader WITH PASSWORD = 'Finserve@2026!', CHECK_POLICY = OFF;   -- change the password if you like
GO
USE CoreBankingDB;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'adf_reader')
    CREATE USER adf_reader FOR LOGIN adf_reader;
ALTER ROLE db_datareader ADD MEMBER adf_reader;
GO
