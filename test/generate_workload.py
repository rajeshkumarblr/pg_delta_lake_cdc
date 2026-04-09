import psycopg2
import os
import random
import time
import sys

def generate_workload():
    conn_info = os.getenv("DATABASE_URL", "postgresql://hn_user:my_hn_pass@postgres:5432/my_hn")
    print(f"Connecting to {conn_info}...")
    
    # Wait for PG to be ready
    retries = 10
    conn = None
    while retries > 0:
        try:
            conn = psycopg2.connect(conn_info)
            break
        except Exception as e:
            print(f"Waiting for Postgres... {e}")
            retries -= 1
            time.sleep(2)
    
    if not conn:
        print("Failed to connect to Postgres")
        sys.exit(1)

    cur = conn.cursor()
    
    # Clean up
    cur.execute("TRUNCATE TABLE integration_test;")
    conn.commit()

    # Insert 10,000 rows
    print("Inserting 10,000 rows...")
    total_inserts = 10000
    scores = []
    for i in range(total_inserts):
        score = random.uniform(0, 1000)
        scores.append(score)
        cur.execute(
            "INSERT INTO integration_test (name, score, is_active) VALUES (%s, %s, %s)",
            (f"User_{i}", score, random.choice([True, False]))
        )
        if i % 1000 == 0:
            conn.commit()
            print(f"Inserted {i} rows...")
    
    conn.commit()
    
    # 1. Rollback Test: These rows should NEVER appear in Delta Lake
    print("Performing transaction ROLLBACK test (10 rows)...")
    cur.execute("BEGIN;")
    for i in range(10):
        row_id = 80000 + i
        cur.execute(
            "INSERT INTO integration_test (id, name, score, is_active) VALUES (%s, %s, %s, %s)",
            (row_id, f"rollback_user_{i}", 0.0, False)
        )
    cur.execute("ROLLBACK;")

    # 2. Atomic multi-row transaction (Success)
    print("Performing atomic multi-row transaction (5 rows)...")
    cur.execute("BEGIN;")
    for i in range(5):
        row_id = 90000 + i
        cur.execute(
            "INSERT INTO integration_test (id, name, score, is_active) VALUES (%s, %s, %s, %s)",
            (row_id, f"tx_user_{i}", 50.0, True)
        )
    cur.execute("COMMIT;")
    
    # 3. Schema Evolution Stage 1: Add 'priority'
    print("Schema Evolution Stage 1: Adding column 'priority'...")
    cur.execute("ALTER TABLE integration_test ADD COLUMN priority INT DEFAULT 0;")
    for i in range(5):
        row_id = 95000 + i
        cur.execute(
            "INSERT INTO integration_test (id, name, score, is_active, priority) VALUES (%s, %s, %s, %s, %s)",
            (row_id, f"evolved_v1_{i}", 75.0, True, 100)
        )
    conn.commit()

    # 4. Schema Evolution Stage 2: Add 'tags'
    print("Schema Evolution Stage 2: Adding column 'tags'...")
    cur.execute("ALTER TABLE integration_test ADD COLUMN tags TEXT DEFAULT 'none';")
    for i in range(5):
        row_id = 96000 + i
        cur.execute(
            "INSERT INTO integration_test (id, name, score, is_active, priority, tags) VALUES (%s, %s, %s, %s, %s, %s)",
            (row_id, f"evolved_v2_{i}", 90.0, True, 200, "production,test")
        )
    conn.commit()

    print("Workload complete. Waiting 15s for final CDC epoch...")
    time.sleep(15)
    # Total Expected = 10000 (initial) + 5 (tx) + 5 (ev1) + 5 (ev2) = 10015
    # Rollback rows (80000-80009) should NOT be counted.
    expected_count = 10015
    cur.execute("SELECT sum(score) FROM integration_test;")
    expected_sum = float(cur.fetchone()[0])
    
    print(f"EXPECTED_COUNT={expected_count}")
    print(f"EXPECTED_SUM={expected_sum:.4f}")
    
    # Write to a file for verification step
    with open("/app/data/expected.txt", "w") as f:
        f.write(f"COUNT={expected_count}\n")
        f.write(f"SUM={expected_sum:.4f}\n")

    cur.close()
    conn.close()

if __name__ == "__main__":
    generate_workload()
