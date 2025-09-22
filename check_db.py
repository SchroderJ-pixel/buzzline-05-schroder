import sqlite3
from pathlib import Path

db_path = Path("data/buzz.sqlite")

if not db_path.exists():
    print(f"Database not found: {db_path}")
else:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Count rows
    cur.execute("SELECT COUNT(*) FROM fitness_events;")
    count = cur.fetchone()[0]
    print(f"Total rows in fitness_events: {count}")

    # Show latest 5 rows
    print("\nLast 5 rows:")
    for row in cur.execute("SELECT ts, author, fitness_tag, effort_score FROM fitness_events ORDER BY id DESC LIMIT 5;"):
        print(row)

    conn.close()
