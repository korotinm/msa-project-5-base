import os
import sys
from datetime import date

import psycopg2
from psycopg2 import sql

TABLE = os.environ.get("EXPORT_TABLE", "shipments")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data")


def main():
    path = os.path.join(OUTPUT_DIR, f"{TABLE}_{date.today():%Y%m%d}.csv")
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    try:
        with conn.cursor() as cur, open(path, "w", newline="") as f:
            query = sql.SQL("COPY (SELECT * FROM {} ORDER BY id) TO STDOUT WITH CSV HEADER").format(
                sql.Identifier(TABLE)
            )
            cur.copy_expert(query, f)
    finally:
        conn.close()

    with open(path) as f:
        rows = sum(1 for _ in f) - 1
    print(f"exported {rows} rows from {TABLE} to {path}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"export failed: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
