import os
import sqlite3
import json

# Try different paths to find database.sqlite
db_path = 'n8n/database.sqlite'
if not os.path.exists(db_path):
    db_path = '../../n8n/database.sqlite'
if not os.path.exists(db_path):
    db_path = '../n8n/database.sqlite'

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]
print("Tables:", tables)

# Query workflow_entity
if 'workflow_entity' in tables:
    cursor.execute("SELECT id, name, active, staticData FROM workflow_entity;")
    print("\nWorkflows in workflow_entity:")
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Name: {row[1]}, Active: {row[2]}")
        # print(f"StaticData: {row[3]}")

# Query webhook_entity if exists
if 'webhook_entity' in tables:
    cursor.execute("SELECT * FROM webhook_entity;")
    print("\nWebhooks in webhook_entity:")
    for row in cursor.fetchall():
        print(row)
