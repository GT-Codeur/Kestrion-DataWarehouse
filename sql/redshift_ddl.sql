-- Star schema for Kestrion (default schema "public" of the Redshift Serverless database).
-- Idempotent: safe to run on every deploy.

-- Dimensions are tiny (8, 120 and a few dozen date rows): DISTSTYLE ALL puts a full copy on every
-- node so joins to the fact never shuffle data.
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id    INT NOT NULL,
  customer_name  VARCHAR(100),
  city           VARCHAR(50),
  category       VARCHAR(50),
  onboarded_date DATE,
  PRIMARY KEY (customer_id)
) DISTSTYLE ALL SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS dim_product (
  product_id   INT NOT NULL,
  sku          VARCHAR(20),
  product_name VARCHAR(100),
  category     VARCHAR(50),
  unit_cost    DECIMAL(10,2),
  qty_on_hand  INT,
  PRIMARY KEY (product_id)
) DISTSTYLE ALL SORTKEY (product_id);

CREATE TABLE IF NOT EXISTS dim_date (
  date_key    INT NOT NULL,
  full_date   DATE,
  cal_year    SMALLINT,
  cal_quarter SMALLINT,
  cal_month   SMALLINT,
  cal_day     SMALLINT,
  day_of_week SMALLINT,
  PRIMARY KEY (date_key)
) DISTSTYLE ALL SORTKEY (date_key);

-- Fact: DISTKEY(order_id) spreads rows evenly and keeps an order's lines together;
-- SORTKEY(date_key) because analyst queries filter and group by date range (zone-map skipping).
CREATE TABLE IF NOT EXISTS fact_order_lines (
  line_id      INT NOT NULL,
  order_id     INT NOT NULL,
  customer_id  INT NOT NULL,
  product_id   INT NOT NULL,
  date_key     INT NOT NULL,
  order_status VARCHAR(20),
  quantity     INT,
  unit_price   DECIMAL(10,2),
  line_amount  DECIMAL(12,2),
  PRIMARY KEY (line_id),
  FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
  FOREIGN KEY (product_id)  REFERENCES dim_product (product_id),
  FOREIGN KEY (date_key)    REFERENCES dim_date (date_key)
) DISTSTYLE KEY DISTKEY (order_id) SORTKEY (date_key)
