/* =====================================================================
   PROJECT 2 - E-COMMERCE : source system (simulates the shop's OLTP DB)
   Products are NOT here - they come from the REST API (dummyjson.com).
   ===================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ecom') EXEC('CREATE SCHEMA ecom');
GO
DROP TABLE IF EXISTS ecom.customers, ecom.customers_late, ecom.orders, ecom.order_items,
                     ecom.payments, ecom.inventory, ecom.returns;

CREATE TABLE ecom.customers (customer_id INT, first_name NVARCHAR(100), last_name NVARCHAR(100), email NVARCHAR(200),
    phone VARCHAR(20), city NVARCHAR(100), state NVARCHAR(100), segment NVARCHAR(30), created_at DATETIME2(0), updated_at DATETIME2(3));
CREATE TABLE ecom.customers_late (customer_id INT, first_name NVARCHAR(100), last_name NVARCHAR(100), email NVARCHAR(200),
    phone VARCHAR(20), city NVARCHAR(100), state NVARCHAR(100), segment NVARCHAR(30), created_at DATETIME2(0), updated_at DATETIME2(3));
CREATE TABLE ecom.orders (order_id BIGINT, customer_id INT, order_ts DATETIME2(0), status NVARCHAR(20), channel NVARCHAR(20), updated_at DATETIME2(3));
CREATE TABLE ecom.order_items (order_item_id BIGINT, order_id BIGINT, product_id INT, quantity INT, unit_price DECIMAL(12,2),
    discount_pct DECIMAL(5,2), updated_at DATETIME2(3));
CREATE TABLE ecom.payments (payment_id BIGINT, order_id BIGINT, method NVARCHAR(20), amount DECIMAL(14,2), status NVARCHAR(20),
    paid_ts DATETIME2(0), updated_at DATETIME2(3));
CREATE TABLE ecom.inventory (warehouse_id VARCHAR(10), product_id INT, qty_on_hand INT, reorder_level INT, updated_at DATETIME2(3));
CREATE TABLE ecom.returns (return_id BIGINT, order_item_id BIGINT, return_ts DATETIME2(0), reason NVARCHAR(50),
    refund_amount DECIMAL(14,2), updated_at DATETIME2(3));
GO
CREATE INDEX ix_c_upd ON ecom.customers(updated_at);   CREATE INDEX ix_o_upd ON ecom.orders(updated_at);
CREATE INDEX ix_oi_upd ON ecom.order_items(updated_at); CREATE INDEX ix_p_upd ON ecom.payments(updated_at);
CREATE INDEX ix_r_upd ON ecom.returns(updated_at);
GO

DELETE FROM ctl.seed_config WHERE project = 'ecom';
INSERT INTO ctl.seed_config (project, landing_folder, file_name, target_schema, target_table) VALUES
('ecom','ecom','customers.csv','ecom','customers'), ('ecom','ecom','customers_late.csv','ecom','customers_late'),
('ecom','ecom','orders.csv','ecom','orders'),       ('ecom','ecom','order_items.csv','ecom','order_items'),
('ecom','ecom','payments.csv','ecom','payments'),   ('ecom','ecom','inventory.csv','ecom','inventory'),
('ecom','ecom','returns.csv','ecom','returns');
GO

/* ---------------------------------------------------------------------
   One "day" of shop activity. Includes:
   - late-arriving customers (they already have orders!)
   - customer moves city / changes segment  -> SCD Type 2 new versions
   - new orders + items + payments, status transitions, stock changes, returns
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE ecom.usp_simulate_daily_changes
    @late_customers INT = 300, @moved_customers INT = 200, @new_orders INT = 2000
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @max_order BIGINT = (SELECT MAX(order_id) FROM ecom.orders);

    -- 1. late-arriving customers move from the "not yet synced" table into customers
    ;WITH l AS (SELECT TOP (@late_customers) * FROM ecom.customers_late
                WHERE customer_id NOT IN (SELECT customer_id FROM ecom.customers) ORDER BY customer_id)
    INSERT INTO ecom.customers SELECT customer_id, first_name, last_name, email, phone, city, state, segment, created_at, @now FROM l;

    -- 2. SCD2 drivers: city move + segment upgrade
    UPDATE c SET city = CASE WHEN city = 'Pune' THEN 'Mumbai' ELSE 'Pune' END,
                 state = 'Maharashtra',
                 segment = CASE WHEN segment IN ('Regular') THEN 'Silver' WHEN segment IN ('Silver','SILVER') THEN 'Gold' ELSE segment END,
                 updated_at = @now
    FROM ecom.customers c
    WHERE c.customer_id IN (SELECT TOP (@moved_customers) customer_id FROM ecom.customers ORDER BY NEWID());

    -- 3. new orders (some for customers that are still "late")
    ;WITH n AS (SELECT TOP (@new_orders) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) i FROM sys.all_objects a CROSS JOIN sys.all_objects b)
    INSERT INTO ecom.orders
    SELECT @max_order + i, 1 + ABS(CHECKSUM(NEWID())) % 50000, DATEADD(MINUTE, -(i % 1440), @now),
           CASE WHEN i % 10 = 0 THEN 'Pending' ELSE 'Shipped' END, CHOOSE(i % 3 + 1, 'Web','App','Store'), @now
    FROM n;

    INSERT INTO ecom.order_items
    SELECT o.order_id * 10 + k.line_no, o.order_id, 1 + ABS(CHECKSUM(NEWID())) % 194, 1 + ABS(CHECKSUM(NEWID())) % 4,
           CAST(5 + ABS(CHECKSUM(NEWID())) % 199500 / 100.0 AS DECIMAL(12,2)), 0, @now
    FROM ecom.orders o CROSS JOIN (VALUES (1),(2)) k(line_no)
    WHERE o.order_id > @max_order;

    INSERT INTO ecom.payments
    SELECT o.order_id, o.order_id, CHOOSE(o.order_id % 4 + 1, 'UPI','Card','COD','Wallet'),
           SUM(i.quantity * i.unit_price), CASE WHEN o.status = 'Pending' THEN 'Pending' ELSE 'Captured' END, o.order_ts, @now
    FROM ecom.orders o JOIN ecom.order_items i ON i.order_id = o.order_id
    WHERE o.order_id > @max_order GROUP BY o.order_id, o.status, o.order_ts;

    -- 4. status transitions on yesterday's orders
    UPDATE ecom.orders SET status = 'Delivered', updated_at = @now
    WHERE order_id IN (SELECT TOP 1500 order_id FROM ecom.orders WHERE status = 'Shipped' ORDER BY NEWID());

    -- 5. stock movement + returns
    UPDATE ecom.inventory SET qty_on_hand = qty_on_hand - ABS(CHECKSUM(NEWID())) % 30, updated_at = @now
    WHERE product_id % 3 = 0;

    INSERT INTO ecom.returns
    SELECT TOP 100 i.order_item_id + 1000000000, i.order_item_id, @now, 'Damaged', i.quantity * i.unit_price, @now
    FROM ecom.order_items i JOIN ecom.orders o ON o.order_id = i.order_id
    WHERE o.status = 'Delivered' AND i.order_item_id NOT IN (SELECT order_item_id FROM ecom.returns) ORDER BY NEWID();

    SELECT 'customers' t, COUNT(*) n FROM ecom.customers WHERE updated_at = @now
    UNION ALL SELECT 'orders', COUNT(*) FROM ecom.orders WHERE updated_at = @now
    UNION ALL SELECT 'order_items', COUNT(*) FROM ecom.order_items WHERE updated_at = @now;
END
GO
