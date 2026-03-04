import sqlite3
conn = sqlite3.connect("integra.db")

conn.execute("""
    CREATE TABLE IF NOT EXISTS task_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_message_id INTEGER NOT NULL,
        tg_chat_id TEXT NOT NULL,
        phone TEXT NOT NULL,
        role TEXT,
        task_text TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_task_msg ON task_messages(tg_message_id, tg_chat_id)")
conn.commit()
conn.close()
print("Migration done: task_messages table created")
