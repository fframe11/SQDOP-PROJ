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

# Update active status
cursor.execute("UPDATE workflow_entity SET active = 1 WHERE id = '1';")
conn.commit()

# Verify
cursor.execute("SELECT id, name, active FROM workflow_entity WHERE id = '1';")
row = cursor.fetchone()
print("Updated Workflow:", row)

conn.close()
