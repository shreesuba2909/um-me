"""
save_ldm.py
-----------
Generates a PowerDesigner Logical Data Model (.ldm) file from DDL SQL
and saves it to disk.

Usage:
    python save_ldm.py
"""

import os
import sys

# ── Make sure the project root is on the path ────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.agents.erd_generator import generate_erd_ldm

# ── Your SQL here ─────────────────────────────────────────────────────────────
SQL = """
CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) UNIQUE
);

CREATE TABLE Orders (
    order_id    INT PRIMARY KEY,
    customer_id INT NOT NULL,
    total       DECIMAL(10,2),
    created_at  TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE OrderItem (
    item_id    INT PRIMARY KEY,
    order_id   INT NOT NULL,
    product    VARCHAR(255) NOT NULL,
    quantity   INT NOT NULL,
    unit_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);
"""

# ── Config ────────────────────────────────────────────────────────────────────
TITLE       = "My Logical Data Model"
OUTPUT_FILE = "output.ldm"          # change path/name if needed


# ── Generate & save ───────────────────────────────────────────────────────────
def main():
    print(f"Generating LDM: '{TITLE}' ...")

    result = generate_erd_ldm(SQL, TITLE)

    if result["error"]:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(result["xml"])

    print(f"Saved   : {os.path.abspath(OUTPUT_FILE)}")
    print(f"Entities: {result['entity_count']}")
    print(f"Relationships: {result['relationship_count']}")


if __name__ == "__main__":
    main()
