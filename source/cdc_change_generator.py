"""
cdc_change_generator.py -- Kestrion Wholesale Distributors (OmniSupply capstone)

Generates an ongoing stream of INSERT / UPDATE / DELETE statements against the
source database (schema.sql) so your DMS ongoing-replication task has real
change-data-capture traffic to pick up. This is a DATA-GENERATION utility only
-- it is not part of the graded pipeline.

Usage (after loading schema.sql and the four seed CSVs into your Aurora/RDS
instance, and starting your DMS task):

    pip install pymysql   # or psycopg2-binary if you used PostgreSQL
    python3 cdc_change_generator.py --host <rds-endpoint> --user admin \\
        --password '<password>' --database kestrion --engine mysql \\
        --duration-minutes 30 --interval-seconds 5

Each cycle randomly does one of:
  - INSERT a new order with 1-4 order_lines (simulates a new sale)
  - UPDATE an existing order's status (placed -> shipped -> delivered), which is
    exactly the kind of change your CDC-resolution logic (section 3 of the
    brief) needs to fold into a current-state table rather than an append-only
    log
  - DELETE a cancelled order's lines and the order itself (simulates an order
    being voided), to exercise delete handling end-to-end

Adjust --interval-seconds and --duration-minutes to control how much change
traffic you generate; a short 10-20 minute run with a 3-5 second interval is
normally enough to demonstrate ongoing replication working correctly.
"""
import argparse, random, time, sys

def get_connection(args):
    if args.engine == "mysql":
        import pymysql
        return pymysql.connect(host=args.host, user=args.user, password=args.password,
                                database=args.database, autocommit=True)
    else:
        import psycopg2
        return psycopg2.connect(host=args.host, user=args.user, password=args.password,
                                 dbname=args.database)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--engine", choices=["mysql", "postgres"], default="mysql")
    ap.add_argument("--duration-minutes", type=int, default=20)
    ap.add_argument("--interval-seconds", type=int, default=5)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    random.seed(args.seed)
    conn = get_connection(args)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT customer_id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, unit_cost FROM inventory")
    products = cur.fetchall()
    cur.execute("SELECT MAX(order_id) FROM orders")
    next_order_id = (cur.fetchone()[0] or 0) + 1
    cur.execute("SELECT MAX(line_id) FROM order_lines")
    next_line_id = (cur.fetchone()[0] or 0) + 1

    deadline = time.time() + args.duration_minutes * 60
    n_inserts = n_updates = n_deletes = 0

    while time.time() < deadline:
        action = random.choices(["insert", "update", "delete"], weights=[50, 40, 10])[0]

        if action == "insert":
            cust = random.choice(customer_ids)
            cur.execute(
                "INSERT INTO orders (order_id, customer_id, order_date, status) VALUES (%s, %s, CURDATE(), 'placed')"
                if args.engine == "mysql" else
                "INSERT INTO orders (order_id, customer_id, order_date, status) VALUES (%s, %s, CURRENT_DATE, 'placed')",
                (next_order_id, cust))
            for _ in range(random.randint(1, 4)):
                pid, cost = random.choice(products)
                qty = random.randint(1, 20)
                cur.execute(
                    "INSERT INTO order_lines (line_id, order_id, product_id, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (next_line_id, next_order_id, pid, qty, cost))
                next_line_id += 1
            next_order_id += 1
            n_inserts += 1

        elif action == "update":
            cur.execute("SELECT order_id, status FROM orders WHERE status IN ('placed','shipped') "
                        "ORDER BY RAND() LIMIT 1" if args.engine == "mysql" else
                        "SELECT order_id, status FROM orders WHERE status IN ('placed','shipped') "
                        "ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                order_id, status = row
                new_status = "shipped" if status == "placed" else "delivered"
                cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (new_status, order_id))
                n_updates += 1

        else:  # delete
            cur.execute("SELECT order_id FROM orders WHERE status='placed' "
                        "ORDER BY RAND() LIMIT 1" if args.engine == "mysql" else
                        "SELECT order_id FROM orders WHERE status='placed' "
                        "ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                order_id = row[0]
                cur.execute("UPDATE orders SET status='cancelled' WHERE order_id=%s", (order_id,))
                cur.execute("DELETE FROM order_lines WHERE order_id=%s", (order_id,))
                cur.execute("DELETE FROM orders WHERE order_id=%s", (order_id,))
                n_deletes += 1

        sys.stdout.write(f"\rinserts={n_inserts} updates={n_updates} deletes={n_deletes}")
        sys.stdout.flush()
        time.sleep(args.interval_seconds)

    print(f"\nDone. inserts={n_inserts} updates={n_updates} deletes={n_deletes}")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
