# ETL-Project

ระบบนี้คือ **SDOQAP - Scalable Data Observability and Quality Assurance Platform** สำหรับทดสอบงาน Data Engineering แบบ end-to-end ตั้งแต่การดึงข้อมูลจากหลายแหล่ง, เก็บลง Data Lake, ตรวจคุณภาพข้อมูลด้วย Spark, บันทึกผลลง Elasticsearch และแสดงผลผ่านเว็บกลางกับ Grafana

แนวคิดหลักของโปรเจกต์คือทำให้ผู้ใช้สามารถรันระบบทั้งหมดได้ด้วยคำสั่งเดียวบน Windows:

```cmd
scripts\maintenance\start_and_test.bat
```

เมื่อรัน script นี้ ระบบจะเปิด container ทั้งหมด, โหลดข้อมูลตัวอย่าง, trigger workflow, ตรวจคุณภาพข้อมูล และเปิดหน้าเว็บกลางให้อัตโนมัติ

## ระบบนี้ทำอะไร

SDOQAP ถูกออกแบบเป็นแพลตฟอร์มตรวจสอบความน่าเชื่อถือของ pipeline ข้อมูล โดยรองรับ flow หลักดังนี้

1. รับข้อมูลจากหลายแหล่ง เช่น CSV file, API source และ database source
2. ใช้ n8n เป็นตัว orchestrate workflow สำหรับ ingestion
3. เก็บ raw data ลง HDFS ภายใต้ `/data/raw`
4. ใช้ Spark ตรวจ schema drift, missing key, duplicate records, missing date และคุณภาพข้อมูลอื่น ๆ
5. แยกข้อมูลดีไปที่ `/data/active` และข้อมูลผิดปกติไปที่ `/data/quarantine`
6. เขียน metadata, quality score, lineage และ pipeline status ลง Elasticsearch
7. แสดงผลผ่าน FastAPI Observability Portal และ Grafana

## Service ที่ใช้

ระบบรันด้วย Docker Compose และประกอบด้วย service หลักเหล่านี้

| Service | หน้าที่ | URL เริ่มต้น |
| --- | --- | --- |
| FastAPI Portal | เว็บกลางสำหรับดูสถานะระบบ, KPI, lineage, quality และ recommendation | `http://localhost:8002/` |
| Grafana | Dashboard สำหรับ observability | `http://localhost:3002/` |
| n8n | Workflow orchestration และ ingestion webhook | `http://localhost:5678/` |
| HDFS NameNode | Data Lake raw/active/quarantine storage | `http://localhost:9870/` |
| Spark Master | ประมวลผล quality validation | `http://localhost:8081/` |
| Elasticsearch | เก็บผลลัพธ์ quality, lineage และ pipeline run | `http://localhost:9200/` |
| Kibana | สำรวจข้อมูลใน Elasticsearch | `http://localhost:5601/` |
| PostgreSQL | Mock OLTP source สำหรับข้อมูล sales records | `localhost:5432` |

Port สามารถแก้ได้ในไฟล์ `.env`

## โครงสร้างข้อมูลที่รองรับ

ระบบรองรับข้อมูลหลายรูปแบบและหลายแหล่ง โดยตัวอย่างที่มีในโปรเจกต์นี้คือ

| Source | ตัวอย่างข้อมูล | วิธีใช้งานในระบบ |
| --- | --- | --- |
| CSV file | sales records, product data, public dataset | mount เข้า n8n แล้วส่งเข้า HDFS |
| API source | mock/public API payload | n8n เรียก API แล้วเขียนผลลง HDFS |
| Database source | PostgreSQL `sales_records` | import CSV เข้า PostgreSQL แล้วให้ workflow ดึงไปตรวจ |
| Extensible source | dataset ใหม่ที่ผู้ใช้เตรียมเอง | เพิ่ม schema ใน `spark/schema_registry.json` หรือให้ Spark infer schema อัตโนมัติ |

Spark quality engine สามารถ infer schema เบื้องต้นได้ ถ้า table ใหม่ยังไม่มีใน registry แต่ถ้าต้องการควบคุม primary key, date column และ expected schema ให้เพิ่ม config ใน `spark/schema_registry.json`

## สิ่งที่ต้องติดตั้งก่อนใช้งาน

เครื่องที่ใช้ทดสอบควรเป็น Windows และมีสิ่งต่อไปนี้

1. Docker Desktop
2. WSL2
3. Ubuntu distribution ใน WSL ชื่อ `Ubuntu`
4. Git
5. RAM แนะนำอย่างน้อย 8 GB เพราะระบบมี Hadoop, Spark, Elasticsearch, Grafana, n8n และ API รันพร้อมกัน

ตรวจสอบ Docker:

```cmd
docker ps
```

ตรวจสอบ WSL:

```cmd
wsl --status
wsl -l -v
```

หมายเหตุ: `start_and_test.bat` มีบางขั้นตอนที่เรียก `wsl -d Ubuntu` ถ้า WSL ของคุณใช้ชื่อ distribution อื่น ให้แก้ชื่อใน script หรือเปลี่ยนชื่อ distribution ให้ตรงกัน

## Dataset สำหรับทดสอบ

ไฟล์ dataset ขนาดใหญ่ไม่ได้ถูก push ขึ้น GitHub เพราะบางไฟล์มีขนาดหลายร้อย MB และไม่เหมาะกับการเก็บใน Git ปกติ หากต้องการทดสอบแบบเต็ม flow ให้เตรียม dataset เองแล้ววางไว้ใน path ต่อไปนี้

```text
stress test/
├─ 100000 Sales Records.csv
├─ import_sales.sql
└─ Olist Brazilian E-Commerce/
   ├─ olist_customers_dataset.csv
   ├─ olist_geolocation_dataset.csv
   ├─ olist_orders_dataset.csv
   ├─ olist_order_items_dataset.csv
   ├─ olist_order_payments_dataset.csv
   ├─ olist_order_reviews_dataset.csv
   ├─ olist_products_dataset.csv
   ├─ olist_sellers_dataset.csv
   └─ product_category_name_translation.csv
```

ไฟล์ `import_sales.sql` อยู่ใน repo แล้ว แต่ CSV ขนาดใหญ่ถูก ignore ไว้ ผู้ใช้ต้องหา dataset มาเองหรือใช้ dataset ที่มี schema ใกล้เคียงแทน

ถ้าต้องการทดสอบเฉพาะระบบหลักโดยไม่ใช้ dataset ใหญ่ สามารถใช้ sample ขนาดเล็กได้ แต่ต้องตรวจสอบว่า workflow ใน n8n และ path ที่ script เรียกใช้ตรงกับชื่อไฟล์จริง

## การติดตั้งครั้งแรก

หลัง clone repo ให้เข้า folder โปรเจกต์

```cmd
cd C:\DataEngProj
```

ถ้ายังไม่มีไฟล์ `.env` ให้รัน installer ก่อน

```cmd
scripts\maintenance\install_sdoqap.bat
```

script นี้จะตรวจ Docker, ตรวจ WSL, สร้าง `.env` เริ่มต้น และ build image ของ FastAPI

ตัวอย่างค่า `.env` เริ่มต้น:

```env
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
KIBANA_PORT=5601
GRAFANA_PORT=3002
N8N_PORT=5678
API_PORT=8002
```

## วิธีรันระบบและทดสอบแบบอัตโนมัติ

รันคำสั่งเดียว:

```cmd
scripts\maintenance\start_and_test.bat
```

สิ่งที่ script ทำให้โดยอัตโนมัติ:

1. อ่านค่า config จาก `.env`
2. สั่ง `docker compose up -d`
3. รอให้ HDFS, Elasticsearch, FastAPI และ n8n พร้อมใช้งาน
4. import workflow ของ n8n ถ้ามีไฟล์ local runtime ที่จำเป็น
5. import mock sales dataset เข้า PostgreSQL
6. trigger n8n webhook เพื่อเริ่ม ingestion
7. รอจนข้อมูลถูกเขียนลง HDFS ครบ
8. submit Spark quality checks สำหรับ `products`, `gov_data` และ `sales_records`
9. เขียนผล quality, lineage และ pipeline run ลง Elasticsearch
10. รัน health check แบบ end-to-end
11. เปิด FastAPI Portal และ Grafana ใน browser

เมื่อ script ทำงานจบ จะเปิดหน้าเหล่านี้ให้อัตโนมัติ:

```text
FastAPI Portal: http://localhost:8002/
Grafana:        http://localhost:3002/
```

ถ้า browser ไม่เปิดเอง ให้เปิด URL ด้านบนด้วยตัวเอง

## วิธีเข้าเว็บกลาง

เว็บกลางคือ FastAPI Observability Portal:

```text
http://localhost:8002/
```

ในหน้านี้จะเห็นข้อมูลสำคัญ เช่น

- สถานะ service ทั้งหมด
- จำนวน record ที่ ingest แล้ว
- global quality score
- จำนวน quarantined records
- anomaly / schema drift
- data lineage trace
- pipeline run status
- recommendation สำหรับแก้ปัญหา

API health check:

```text
http://localhost:8002/health
```

ตัวอย่าง API endpoint:

```text
http://localhost:8002/api/v1/services/status
http://localhost:8002/api/v1/kpi/stats
http://localhost:8002/api/v1/quality
http://localhost:8002/api/v1/pipeline
http://localhost:8002/api/v1/lineage/products
```

## วิธีเข้า Grafana

เปิด:

```text
http://localhost:3002/
```

ค่า login เริ่มต้น:

```text
username: admin
password: admin
```

Grafana ถูก provision datasource ไปที่ Elasticsearch แล้ว สามารถใช้ดูข้อมูล quality run, lineage run และ pipeline run ได้

## วิธีตรวจว่าระบบทำงานครบ

หลัง `start_and_test.bat` จบ สามารถตรวจ manual ได้ด้วยคำสั่งเหล่านี้

ดู container:

```cmd
docker compose ps
```

ดู HDFS raw data:

```cmd
docker exec sdoqap-namenode hdfs dfs -ls /data/raw
```

ดู HDFS active data:

```cmd
docker exec sdoqap-namenode hdfs dfs -ls /data/active
```

ดู HDFS quarantine data:

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

## การเพิ่ม dataset ใหม่

ถ้าต้องการทดสอบข้อมูลชุดใหม่ ให้ทำตามแนวทางนี้

1. วางไฟล์ไว้ใน folder ที่ n8n หรือ script เข้าถึงได้ เช่น `stress test/`
2. ปรับ n8n workflow ให้ ingest dataset นั้นลง HDFS path รูปแบบนี้:

```text
/data/raw/<table_name>
```

3. เพิ่ม schema config ใน `spark/schema_registry.json`

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

4. รัน Spark quality check:

```cmd
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py orders
```

5. เปิดเว็บกลางเพื่อดูผล:

```text
http://localhost:8002/
```

ถ้าไม่เพิ่ม schema registry ระบบจะพยายาม infer schema, primary key และ date column เอง แต่ผลอาจไม่ตรงกับ business rule ที่ต้องการ

## การทดสอบเฉพาะบางส่วน

รัน health check:

```cmd
wsl -d Ubuntu -u root bash /mnt/c/DataEngProj/scripts/system_health_check.sh
```

รัน Spark check ด้วยตัวเอง:

```cmd
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py sales_records
```

ดู log ของ service:

```cmd
docker logs sdoqap-api
docker logs sdoqap-n8n
docker logs sdoqap-spark-master
docker logs sdoqap-elasticsearch
```

## หมายเหตุสำหรับผู้ clone จาก GitHub

repo นี้ไม่เก็บไฟล์ต่อไปนี้ด้วยเหตุผลด้านความปลอดภัยและขนาดไฟล์

- `.env`
- `n8n/credentials.json`
- `n8n/database.sqlite`
- CSV dataset ขนาดใหญ่ใน `stress test/`

ดังนั้นถ้า clone จาก GitHub แล้วต้องการรัน full automated test ต้องเตรียมไฟล์เหล่านี้ให้ครบก่อน หรือปรับ workflow ให้ใช้ credential และ dataset ของคุณเอง

ถ้าเป็นเจ้าของโปรเจกต์ที่มีไฟล์ local ครบอยู่แล้ว สามารถรัน `scripts\maintenance\start_and_test.bat` ได้เลย

## การปิดระบบ

หยุด container:

```cmd
docker compose down
```

หยุดและลบ volume ทั้งหมด:

```cmd
docker compose down -v
```

คำสั่ง `down -v` จะลบข้อมูลใน PostgreSQL, Elasticsearch, HDFS และ n8n volume ทั้งหมด ควรใช้เมื่ออยาก reset ระบบเท่านั้น

## Folder สำคัญ

```text
api/                         FastAPI serving layer และเว็บกลาง
docs/                        เอกสาร architecture และ system design
grafana/provisioning/        Grafana datasource provisioning
n8n/                         n8n workflow export
scripts/maintenance/         script ติดตั้ง, start, cleanup
scripts/system_health_check.sh
spark/                       Spark quality engine และ schema registry
stress test/                 dataset สำหรับทดสอบแบบ local
docker-compose.yml           service orchestration
```

## License

MIT © 2024-2026 fframe11
