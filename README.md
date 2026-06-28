# ETL-Project

ระบบนี้คือ **SDOQAP - Scalable Data Observability and Quality Assurance Platform** สำหรับทดสอบงาน Data Engineering แบบ end-to-end ตั้งแต่รับข้อมูลจากหลายแหล่ง, เก็บลง Data Lake, ตรวจคุณภาพข้อมูลด้วย Spark, บันทึกผลลง Elasticsearch และแสดงผลผ่านเว็บกลางกับ Grafana

สำหรับผู้ที่ clone repo ไปใช้งาน ให้ใช้ 2 script หลักนี้:

```cmd
scripts\maintenance\start_and_test.bat
scripts\maintenance\test_data_source.bat
```

`start_and_test.bat` ใช้เปิดระบบอย่างเดียว ส่วน `test_data_source.bat` ใช้เลือกว่าจะทดสอบด้วยไฟล์ dataset หรือ API URL

## ภาพรวมระบบ

SDOQAP ทำงานเป็น data observability platform ที่รับข้อมูลจากหลาย source แล้วตรวจคุณภาพข้อมูลแบบอัตโนมัติ

1. รับข้อมูลจาก CSV dataset หรือ API
2. โหลด raw data เข้า HDFS ที่ `/data/raw/<table_name>`
3. ใช้ Spark ตรวจ schema, primary key, duplicate records, missing value และ date freshness
4. แยกข้อมูลดีไปที่ `/data/active/<table_name>`
5. แยกข้อมูลที่ไม่ผ่าน rule ไปที่ `/data/quarantine/<table_name>`
6. เขียนผล quality, lineage และ pipeline status ลง Elasticsearch
7. แสดงผลผ่าน FastAPI Central Portal และ Grafana

## Service หลัก

| Service | หน้าที่ | URL เริ่มต้น |
| --- | --- | --- |
| FastAPI Central Portal | เว็บกลางสำหรับดูสถานะระบบ, KPI, quality, lineage และ pipeline run | `http://localhost:8002/` |
| Grafana | Dashboard สำหรับ observability | `http://localhost:3002/` |
| n8n | Workflow orchestration สำหรับ flow ที่ต้องใช้ n8n | `http://localhost:5678/` |
| HDFS NameNode | Data Lake สำหรับ raw/active/quarantine data | `http://localhost:9870/` |
| Spark Master | ประมวลผล quality validation | `http://localhost:8081/` |
| Elasticsearch | เก็บผลลัพธ์ quality, lineage และ pipeline run | `http://localhost:9200/` |
| Kibana | สำรวจข้อมูลใน Elasticsearch | `http://localhost:5601/` |
| PostgreSQL | Mock database source | `localhost:5432` |

Port สามารถแก้ได้ใน `.env`

## สิ่งที่ต้องติดตั้ง

แนะนำให้รันบน Windows และติดตั้งสิ่งต่อไปนี้ก่อน

1. Docker Desktop
2. WSL2
3. Git
4. RAM อย่างน้อย 8 GB

ตรวจ Docker:

```cmd
docker ps
```

ถ้ายังไม่มี `.env` ให้รัน:

```cmd
scripts\maintenance\install_sdoqap.bat
```

## วิธีใช้งานสำหรับผู้ clone repo

### 1. เปิดระบบ

รัน:

```cmd
scripts\maintenance\start_and_test.bat
```

script นี้จะทำเฉพาะงาน start platform:

1. อ่าน config จาก `.env`
2. สั่ง `docker compose up -d`
3. import n8n workflow ถ้ามีไฟล์พร้อม
4. รอ HDFS, Elasticsearch, FastAPI และ n8n พร้อมใช้งาน
5. เปิด Central Portal และ Grafana

script นี้ **ไม่รันทดสอบข้อมูลให้อัตโนมัติแล้ว** เพื่อให้ผู้ใช้เลือกเองว่าจะ test ด้วย dataset หรือ API

หลังระบบพร้อม ให้เปิดเว็บกลาง:

```text
http://localhost:8002/
```

Grafana:

```text
http://localhost:3002/
username: admin
password: admin
```

### 2. เลือกทดสอบด้วย Dataset หรือ API

รัน:

```cmd
scripts\maintenance\test_data_source.bat
```

ใน script จะมีเมนู:

```text
1. Test with local CSV dataset
2. Test with API URL
3. Open Central Portal
4. Show HDFS raw datasets
5. Exit
```

## จุดวางไฟล์ Dataset

ให้วางไฟล์ CSV ที่ต้องการทดสอบไว้ที่:

```text
user_inputs/datasets/
```

ตัวอย่าง:

```text
user_inputs/datasets/orders.csv
user_inputs/datasets/customers.csv
user_inputs/datasets/sales_records.csv
```

จากนั้นรัน:

```cmd
scripts\maintenance\test_data_source.bat
```

เลือก option `1. Test with local CSV dataset`

ระบบจะถาม:

```text
Enter table name for HDFS/Spark:
Enter CSV file name or full path:
```

ตัวอย่างการกรอก:

```text
Enter table name for HDFS/Spark: orders
Enter CSV file name or full path: orders.csv
```

script จะทำให้โดยอัตโนมัติ:

1. copy CSV เข้า container ของ HDFS NameNode
2. สร้าง HDFS path `/data/raw/orders`
3. upload ไฟล์เป็น `/data/raw/orders/orders.csv`
4. run Spark quality engine ด้วย table name `orders`
5. เขียนผลลัพธ์ลง Elasticsearch
6. ดูผลได้ที่ Central Portal

## จุดทดสอบ API

ถ้ามี API URL ให้รัน:

```cmd
scripts\maintenance\test_data_source.bat
```

เลือก option `2. Test with API URL`

ระบบจะถาม:

```text
Enter table name for HDFS/Spark:
Enter API URL:
```

ตัวอย่าง:

```text
Enter table name for HDFS/Spark: gov_data
Enter API URL: https://example.com/api/data
```

script จะ download API response แล้วแปลงเป็น CSV เท่าที่ทำได้ จากนั้นบันทึกไว้ที่:

```text
user_inputs/apis/<table_name>.csv
```

รูปแบบ API ที่รองรับ:

- JSON array
- JSON object
- JSON object ที่มี `result.records`
- JSON object ที่มี `records`, `data`, หรือ `items`
- raw CSV response

หลังแปลงเสร็จ script จะ upload CSV เข้า HDFS และรัน Spark quality check เหมือนกรณี dataset

## การกำหนด Schema สำหรับ Dataset ใหม่

Spark quality engine สามารถ infer schema เบื้องต้นได้ แต่ถ้าต้องการผลที่แม่นขึ้น ให้เพิ่ม config ใน:

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

ถ้าไม่เพิ่ม schema ระบบจะเลือก primary key และ date column เองจากชื่อ column เช่น `id`, `<table>_id`, `date`, `created_at`, `updated_at`, `timestamp`

## วิธีตรวจว่าระบบทำงาน

ดู container:

```cmd
docker compose ps
```

ดู raw data ใน HDFS:

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

ดู API health:

```cmd
curl http://localhost:8002/health
```

## API Endpoint สำคัญ

```text
http://localhost:8002/health
http://localhost:8002/api/v1/services/status
http://localhost:8002/api/v1/kpi/stats
http://localhost:8002/api/v1/quality
http://localhost:8002/api/v1/pipeline
http://localhost:8002/api/v1/lineage/products
```

## ไฟล์ที่ไม่ได้ push ขึ้น GitHub

repo นี้ไม่เก็บไฟล์ต่อไปนี้ด้วยเหตุผลด้านความปลอดภัยและขนาดไฟล์

- `.env`
- `n8n/credentials.json`
- `n8n/database.sqlite`
- CSV dataset ขนาดใหญ่ใน `stress test/`
- ไฟล์ที่ผู้ใช้วางเองใน `user_inputs/datasets/`
- ไฟล์ API output ใน `user_inputs/apis/`

ถ้าต้องการทดสอบ ให้เตรียม dataset หรือ API ของตัวเอง แล้วใช้ `test_data_source.bat`

## การปิดระบบ

หยุด container:

```cmd
docker compose down
```

หยุดและลบ volume ทั้งหมด:

```cmd
docker compose down -v
```

คำสั่ง `down -v` จะลบข้อมูลใน PostgreSQL, Elasticsearch, HDFS และ n8n volume ทั้งหมด ใช้เมื่อต้องการ reset ระบบ

## Folder สำคัญ

```text
api/                         FastAPI serving layer และเว็บกลาง
docs/                        เอกสาร architecture และ system design
grafana/provisioning/        Grafana datasource provisioning
n8n/                         n8n workflow export
scripts/maintenance/         start/test/install scripts
spark/                       Spark quality engine และ schema registry
user_inputs/datasets/        จุดวาง CSV dataset สำหรับผู้ใช้
user_inputs/apis/            จุดเก็บ API output สำหรับผู้ใช้
docker-compose.yml           service orchestration
```

## License

MIT © 2024-2026 fframe11
