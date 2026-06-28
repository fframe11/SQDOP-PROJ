import os
import sqlite3

# Try different paths to find database.sqlite
db_path = 'n8n/database.sqlite'
if not os.path.exists(db_path):
    db_path = '../../n8n/database.sqlite'
if not os.path.exists(db_path):
    db_path = '../n8n/database.sqlite'

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]

if 'webhook_entity' in tables:
    cursor.execute("SELECT * FROM webhook_entity;")
    print("Registered webhooks in DB:")
    for row in cursor.fetchall():
        print(row)
else:
    print("webhook_entity table not found")

conn.close()
