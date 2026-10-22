-- schema.sql -- Kestrion Wholesale Distributors order-management schema
-- Run this against the Aurora/RDS (MySQL- or PostgreSQL-compatible) instance
-- you provision in the Learner Lab, then load customers.csv / inventory.csv /
-- orders.csv / order_lines.csv as the initial full-load snapshot before
-- starting DMS ongoing replication.

CREATE TABLE customers (
    customer_id     INT PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    city            VARCHAR(50),
    category        VARCHAR(50),
    onboarded_date  DATE
);

CREATE TABLE inventory (
    product_id      INT PRIMARY KEY,
    sku             VARCHAR(20) NOT NULL,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    unit_cost       DECIMAL(10,2),
    qty_on_hand     INT
);

CREATE TABLE orders (
    order_id        INT PRIMARY KEY,
    customer_id     INT NOT NULL REFERENCES customers(customer_id),
    order_date      DATE NOT NULL,
    status          VARCHAR(20) NOT NULL  -- placed | shipped | delivered | cancelled
);

CREATE TABLE order_lines (
    line_id         INT PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES orders(order_id),
    product_id      INT NOT NULL REFERENCES inventory(product_id),
    quantity        INT NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL
);

-- Recommended: enable binary logging / logical replication (per the DMS
-- source-endpoint setup guide for your engine) BEFORE running the CDC
-- change generator, so DMS's ongoing-replication task captures every change.
