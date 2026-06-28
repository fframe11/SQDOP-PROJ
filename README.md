# ETL-Project

ระบบนี้คือ **SDOQAP - Scalable Data Observability and Quality Assurance Platform** สำหรับทดสอบงาน Data Engineering แบบ end-to-end: รับข้อมูลจาก Dataset หรือ API, เก็บเข้า HDFS, ตรวจคุณภาพด้วย Spark, บันทึกผลลง Elasticsearch และดูผลผ่านเว็บกลางกับ Grafana

## ใช้งานหลัก 2 ไฟล์

หลัง clone repo ผู้ใช้จะเห็น script อยู่หน้าโปรเจกต์เลย:

```cmd
start_system.bat
test_data_source.bat
```

ใช้ตามลำดับนี้:

1. `start_system.bat` เปิดระบบทั้งหมด
2. `test_data_source.bat` เลือกว่าจะเทสด้วยไฟล์ dataset หรือ API URL

## Start ระบบ

รัน:

```cmd
start_system.bat
```

script นี้จะ:

1. start Docker Compose services
2. รอ HDFS, Elasticsearch, API และ n8n พร้อมใช้งาน
3. เปิดเว็บกลางและ Grafana

เว็บกลาง:

```text
http://localhost:8002/
```

Grafana:

```text
http://localhost:3002/
username: admin
password: admin
```

## Test ด้วย Dataset หรือ API

หลังระบบเปิดแล้ว ให้รัน:

```cmd
test_data_source.bat
```

เมนูที่มี:

```text
1. Test with local CSV dataset
2. Test with API URL
3. Open Central Portal
4. Show HDFS raw datasets
5. Exit
```

## กรณีเลือก Dataset

วางไฟล์ CSV ไว้หน้าโปรเจกต์ เช่น:

```text
DataEngProj/
├─ start_system.bat
├─ test_data_source.bat
├─ orders.csv
└─ docker-compose.yml
```

จากนั้นรัน `test_data_source.bat` เลือกข้อ `1` แล้วพิมพ์ชื่อไฟล์ได้เลย:

```text
Enter table name for HDFS/Spark: orders
Enter CSV file name or full path: orders.csv
```

หรือจะวางไฟล์ไว้ใน:

```text
user_inputs/datasets/
```

แล้วพิมพ์ชื่อไฟล์อย่างเดียวก็ได้เช่นกัน:

```text
Enter CSV file name or full path: orders.csv
```

script จะ upload ไฟล์เข้า HDFS ที่:

```text
/data/raw/orders/orders.csv
```

จากนั้นจะรัน Spark quality check และส่งผลไปที่ Elasticsearch เพื่อดูในเว็บกลาง

## กรณีเลือก API

รัน `test_data_source.bat` เลือกข้อ `2` แล้วกรอก API URL ใน terminal ได้เลย:

```text
Enter table name for HDFS/Spark: gov_data
Enter API URL: https://example.com/api/data
```

script จะ download API response, แปลงเป็น CSV เท่าที่ทำได้ แล้วเก็บไว้ที่:

```text
user_inputs/apis/<table_name>.csv
```

รูปแบบ API ที่รองรับ:

- JSON array
- JSON object
- JSON object ที่มี `result.records`
- JSON object ที่มี `records`, `data`, หรือ `items`
- raw CSV response

หลังจากนั้นระบบจะ upload เข้า HDFS และรัน Spark quality check เหมือนกรณี dataset

## Service หลัก

| Service | URL |
| --- | --- |
| Central Portal | `http://localhost:8002/` |
| Grafana | `http://localhost:3002/` |
| n8n | `http://localhost:5678/` |
| HDFS NameNode | `http://localhost:9870/` |
| Spark Master | `http://localhost:8081/` |
| Elasticsearch | `http://localhost:9200/` |
| Kibana | `http://localhost:5601/` |

## Schema สำหรับข้อมูลใหม่

ถ้าต้องการกำหนด primary key, date column หรือ expected schema ให้เพิ่ม config ใน:

```text
spark/schema_registry.json
```

ตัวอย่าง:

```json
{
  "orders": {
    "primary_key": "order_id",
    "date_column": "order_date",
    "schema_spec": {
      "order_id": "StringType",
      "customer_id": "StringType",
      "order_date": "TimestampType",
      "amount": "DoubleType"
    }
  }
}
```

ถ้าไม่เพิ่ม schema ระบบจะ infer schema เบื้องต้นให้อัตโนมัติ

## ตรวจระบบ

ดู container:

```cmd
docker compose ps
```

ดู raw data:

```cmd
docker exec sdoqap-namenode hdfs dfs -ls /data/raw
```

ดู active data:

```cmd
docker exec sdoqap-namenode hdfs dfs -ls /data/active
```

ดู quarantine data:

```cmd
docker exec sdoqap-namenode hdfs dfs -ls /data/quarantine
```

ดู Elasticsearch indices:

```cmd
curl http://localhost:9200/_cat/indices?v
```

## ไฟล์ที่ไม่ควร push ขึ้น Git

repo ตั้งค่า ignore ไว้แล้วสำหรับ:

- `.env`
- root-level CSV เช่น `orders.csv`
- `user_inputs/datasets/*`
- `user_inputs/apis/*`
- `n8n/credentials.json`
- `n8n/database.sqlite`
- dataset ใหญ่ใน `stress test/`

## ปิดระบบ

หยุด container:

```cmd
docker compose down
```

reset volume ทั้งหมด:

```cmd
docker compose down -v
```

## License

MIT © 2024-2026 fframe11
