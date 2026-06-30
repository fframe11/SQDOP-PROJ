# generate_dummy_data.py
"""Utility to create dummy test files in various formats.
Supported formats: CSV, JSON, Parquet, Avro, XML.
Generated files are placed in the sibling `dummy_data/` folder.
"""
import os, json, csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_csv(path):
    rows = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.0},
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "value"])
        writer.writeheader()
        writer.writerows(rows)

def write_json(path):
    data = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.0},
    ]
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def write_parquet(path):
    df = pd.DataFrame([
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.0},
    ])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

def write_avro(path):
    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "float"},
        ],
    }
    records = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.0},
    ]
    with open(path, "wb") as out:
        fastavro.writer(out, schema, records)

def write_xml(path):
    xml_content = """<?xml version='1.0' encoding='UTF-8'?>\n<records>\n  <record>\n    <id>1</id>\n    <name>Alice</name>\n    <value>10.5</value>\n  </record>\n  <record>\n    <id>2</id>\n    <name>Bob</name>\n    <value>20.0</value>\n  </record>\n</records>\n"""
    with open(path, "w") as f:
        f.write(xml_content)

if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "..", "dummy_data")
    ensure_dir(base)
    write_csv(os.path.join(base, "sample.csv"))
    write_json(os.path.join(base, "sample.json"))
    write_parquet(os.path.join(base, "sample.parquet"))
    write_avro(os.path.join(base, "sample.avro"))
    write_xml(os.path.join(base, "sample.xml"))
    print("Dummy data generated in", base)
