# เอกสารอธิบายรายละเอียดและกลไกการทำงานของระบบเชิงลึก (Detailed System Breakdown & Logic)
## โครงการระบบบริหารจัดการความน่าเชื่อถือและติดตามเส้นทางข้อมูลขนาดใหญ่ (SDOQAP)

เอกสารนี้อธิบายการทำงานภายใน (Internal Logic) ของระบบ **SDOQAP** ทั้งหมดอย่างละเอียด ตั้งแต่จุดเริ่มต้นการรันระบบ (System Startup) ผ่านกลไกการรับข้อมูล (Ingestion) การประมวลผลด้วย Apache Spark การจัดการธรรมาภิบาลข้อมูล (Data Governance) ตลอดจนการสังเกตการณ์ (Observability) และการส่งมอบข้อมูลสำเร็จรูป

---

## 1. ภาพรวมขั้นตอนการทำงานของระบบตั้งแต่เริ่มต้นจนสิ้นสุด (End-to-End Execution Flow)

การทำงานของระบบ SDOQAP แบ่งออกเป็น 6 ขั้นตอนหลัก นับตั้งแต่การเปิดสวิตช์ระบบจนถึงปลายน้ำ ดังแผนผังลำดับขั้นต่อไปนี้:

```
[ 1. System Startup ]
         │
         ▼
[ 2. Data Ingestion ] ──► (Local CSV / REST API / Reddit Live Stream)
         │
         ▼
[ 3. Distributed Processing (Spark Engine) ]
         │
         ├─► [3.1 Acquire Distributed Lock (ES)]
         ├─► [3.2 Track Size Detection & Parameter Tuning]
         ├─► [3.3 Column Standardization & Safe Type Promotion]
         ├─► [3.4 Schema Drift Governance (Auto-approve vs Proposal Gate)]
         ├─► [3.5 Cleanse & Row-Level Validation (Null / Duplicates / Outliers)]
         └─► [3.6 Atomic Write to HDFS (Delta Lake MERGE / Overwrite)]
         │
         ▼
[ 4. Metadata Logs & Alerting ] ──► (Index to ES & Trigger Alerts to n8n/Slack)
         │
         ▼
[ 5. Gold Layer Aggregation ] ──► (Pre-aggregate into ES Gold Indices)
         │
         ▼
[ 6. Serving Layer & UI ] ──► (FastAPI Serving, Trust-Check API, React Dashboard)
```

---

## 2. รายละเอียดส่วนประกอบหลักของระบบ (Core Components Detail)

### 2.1 Ingestion Core (ส่วนนำเข้าข้อมูล)
ส่วนงานนำเข้าข้อมูลทำหน้าที่ดึงข้อมูลดิบจากโลกภายนอก (Data Sources) เข้าสู่ HDFS โดยมี 3 รูปแบบหลัก:
1. **Local CSV Dataset**: นำเข้าไฟล์ผ่านสคริปต์ `test_data_source.bat` (ข้อ 1) ระบบจะคัดลอกไฟล์ขึ้น HDFS NameNode โดยตรง
2. **REST API**: ผู้ใช้ระบุ API URL ผ่านระบบ `test_data_source.bat` (ข้อ 2) ระบบจะส่งคำขอ HTTP Web Request ผ่าน PowerShell เพื่อดาวน์โหลดข้อมูล จากนั้นตรวจสอบโครงสร้างของ JSON Payload (เช่น JSON Array หรือ Object ที่มี field `records`, `result.records`, `data`, หรือ `items`) แล้วทำการแปลงให้อยู่ในรูปไฟล์ CSV ทันทีเพื่อนำเข้า HDFS
3. **Reddit Live Stream (Streaming Track)**: รันสคริปต์ `scripts/reddit_stream.py` เพื่อดึงข้อมูลโพสต์สดจาก Reddit API แบบ Real-time จากนั้นส่งเข้าคิว **Apache Kafka (sdoqap-kafka)** และรันงาน Spark Streaming (`streaming_job.py`) เพื่ออ่านข้อมูลจาก Kafka เข้าประมวลผลอย่างต่อเนื่อง

### 2.2 Storage Infrastructure (ส่วนจัดเก็บข้อมูลบน HDFS)
จัดเก็บข้อมูลกระจายศูนย์ด้วย **Apache Hadoop HDFS** ซึ่งแบ่งออกเป็น 3 โซน (Medallion Architecture + Quarantine Zone):
- **Bronze Layer (`/data/raw/<table_name>/`)**: เก็บข้อมูลดิบไม่ว่าจะเป็น CSV หรือไฟล์ผลลัพธ์จาก API โดยไม่ทำความสะอาด
- **Silver Layer (`/data/active/<table_name>/`)**: เก็บข้อมูลที่ล้างและตรวจสอบผ่านเกณฑ์คุณภาพเรียบร้อยแล้ว จัดเก็บในรูปแบบ **Delta Lake Format** ซึ่งรองรับ ACID Transactions
- **Quarantine Zone (`/data/quarantine/<table_name>/`)**: เก็บแถวข้อมูลที่ไม่ผ่านเกณฑ์คุณภาพหรือเป็นค่า Outlier/Anomaly โดยมีการจำแนกสาเหตุ (Reject Reason) บันทึกคู่กับ `run_id` เพื่อรองรับการทำงานใหม่ (Retry)

### 2.3 Distributed Processing Engine (ส่วนประมวลผล Spark Cluster)
ประมวลผลข้อมูลในระดับแถวด้วย **Apache Spark** ซึ่งถูกออกแบบให้ทำงานแบบ Dockerized Cluster:
- **sdoqap-spark-master**: ตัวควบคุมหลัก รับคำสั่งประมวลผลผ่าน `spark-submit`
- **sdoqap-spark-worker**: ตัวประมวลผลประจักษ์พยาน ทำงานแบบกระจายหน่วยความจำขนาน

### 2.4 Metadata & Logging Layer (Elasticsearch & Kibana)
เก็บข้อมูลเชิงสังเกตการณ์ (Observability) ไว้ใน **Elasticsearch** ประกอบด้วยดัชนี (Indices) ดังนี้:
- `sdoqap_pipeline_runs`: เก็บสถานะความสำเร็จ/ล้มเหลวของรอบการทำงาน
- `sdoqap_quality_runs`: เก็บค่า Quality Score, จำนวนเรคคอร์ดทั้งหมด, เรคคอร์ดสะอาด, และเรคคอร์ดที่ถูกกักกัน
- `sdoqap_schema_drifts`: เก็บบริบทการเปลี่ยนแปลงโครงสร้างคอลัมน์
- `sdoqap_schema_proposals`: เก็บสถานะโครงสร้างข้อมูลที่รออนุมัติ
- `sdoqap_run_locks`: เก็บสถานะการทำงานเพื่อทำ Distributed Lock

### 2.5 Serving API & Custom UI (แผงควบคุมและส่วนบริการข้อมูล)
- **FastAPI serving layer**: บริการ REST API ที่พอร์ต `8002` (ผ่าน Nginx Reverse Proxy พอร์ต `80`) เพื่อเข้าถึง Elasticsearch และ HDFS
- **React.js Frontend Portal**: แสดงผลภาพรวม KPI แผนผัง Lineage อนิเมชัน สถิติข้อผิดพลาด และกล่องเครื่องมือสำหรับ Data Engineer

---

## 3. ตรรกะและกระบวนการทำงานใน Spark Quality Engine (Spark Processing Logic Deep Dive)

เมื่อสคริปต์ `spark_quality_engine.py` ทำงาน ระบบจะมีตรรกะในการประมวลผลและตัดสินใจเชิงคุณภาพ (Data Reliability Logic) ดังนี้:

### 3.1 Distributed Lock Mechanism (ระบบป้องกันการรันงานซ้ำซ้อน)
ระบบใช้ Elasticsearch ทำหน้าที่เป็น Distributed Lock Manager เพื่อความเสถียรเมื่อรันงานแบบกระจายศูนย์:
- **การขอสิทธิ์ล็อก (Acquire Lock)**: Spark ส่งคำขอ HTTP PUT ไปที่ดัชนี `sdoqap_run_locks/_doc/<table_name>?op_type=create` หากมีการรันงานของตารางนี้ค้างอยู่ Elasticsearch จะส่งสถานะ 409 Conflict กลับมา ทำให้ Spark ยกเลิกรันรอบนั้นเพื่อป้องกันปัญหาระบบเขียนทับกัน
- **ระบบเยียวยาตัวเองของล็อก (Self-Healing & Expirations)**: หากรอบการทำงานก่อนหน้าแครชกระทันหันและล็อกไม่ได้ปลด ระบบจะตรวจสอบ `expires_at` (มีอายุ 15 นาที) หากหมดเวลาแล้ว ระบบจะทำ **Optimistic Concurrency Control (OCC)** โดยดึงเลข `_seq_no` และ `_primary_term` มารันคำสั่ง PUT ทับเพื่อแย่งชิงสิทธิ์แบบอะตอม (Atomic Overwrite)
- **ตัวคุ้มกันล็อก (Lock Protector)**: ใช้ Python Decorator `@lock_protector` ครอบคลุมฟังก์ชันทำงาน เพื่อรับประกันว่าเมื่อ Spark รันล้มเหลวหรือถูกอินเทอร์รัปต์ ล็อกจะถูกลบออกจาก ES เสมอผ่านคำสั่ง DELETE

### 3.2 Size-Based Shuffle Partition Tuning (การเปลี่ยนเกียร์ประมวลผลตามขนาดไฟล์)
เพื่อลดระยะเวลาประมวลผลและการใช้ทรัพยากร:
- ระบบใช้ Hadoop FileSystem API ตรวจสอบขนาดความจุข้อมูลในไดเรกทอรีดิบ `/data/raw/<table_name>`
- หากขนาดข้อมูลน้อยกว่า **50 MB** ระบบจะจัดประเภทเป็น **Fast Track** และปรับแต่งให้ Spark ใช้จำนวนพาร์ทิชันย่อย (`spark.sql.shuffle.partitions`) เป็น **2** เพื่อหลีกเลี่ยงการเสียเวลาไปกับการย้ายข้อมูลข้ามโหนดแบบกระจาย (Shuffle Overhead)
- หากมีขนาดใหญ่กว่าเกณฑ์ จะจัดเป็น **Batch Track** และตั้งค่าจำนวนพาร์ทิชันย่อยเป็น **10** (หรือมากกว่า) เพื่อให้เวิร์กเกอร์ช่วยกันประมวลผลอย่างเต็มขีดความสามารถ

### 3.3 Column Standardization & Safe Type Promotion (การปรับระดับชนิดข้อมูลอัจฉริยะ)
1. **Column Name Standardization**: แปลงชื่อคอลัมน์ต้นทางด้วย Regex ให้เป็นพิมพ์เล็กและตัดสัญลักษณ์พิเศษออก เพื่อทำ Fuzzy Matching จับคู่กับ Schema สเปกเดิม
2. **Safe Type Promotion (การเลื่อนขั้นชนิดข้อมูลแบบปลอดภัย)**:
   - ปัญหาพบบ่อยคือ คอลัมน์ถูกตั้งสเปกเป็นตัวเลขจำนวนเต็ม (`IntegerType`) แต่ชุดข้อมูลรอบใหม่ส่งค่าทศนิยมมา ซึ่งตามปกติหากแปลง (Cast) เป็น integer ตรงๆ จะทำให้ค่าสูญเสียความแม่นยำ หรือกลายเป็น NULL
   - **กลไกการสแกนทศนิยมขนาน**: Spark จะทำการ Aggregation ตรวจสอบฟิลด์ที่เป็น IntegerType ทั้งหมดก่อน หากพบว่ามีแถวที่มีค่าทศนิยมปนอยู่ (เช็คด้วย Regex `.0-9`) ระบบจะทำ **Safe Type Promotion** เลื่อนขั้นคอลัมน์นั้นให้เป็นชนิดเลขทศนิยม (`DoubleType`) ในหน่วยความจำทันทีพร้อมบันทึกประวัติ

### 3.4 Schema Drift Governance Evolution Gate (การคัดกรองการเปลี่ยนโครงสร้างข้อมูล)
ระบบตรวจสอบว่ามีโครงสร้างคอลัมน์เปลี่ยนไปจากที่ระบุใน `schema_registry.json` หรือไม่:
- **Missing Columns (คอลัมน์หาย)**: ตรวจสอบและกรอกค่า NULL ให้อัตโนมัติในหน่วยความจำ พร้อมส่งแจ้งเตือนระดับวิกฤต (Critical Alert) ไปที่ n8n
- **Type Mismatch (ชนิดข้อมูลไม่ตรง)**: หากชนิดข้อมูลต่างกันอย่างรุนแรง ระบบจะบังคับแปลงคอลัมน์นั้นเป็น `StringType` ทันทีเพื่อประคองให้ระบบหลักไม่หยุดทำงาน พร้อมส่งแจ้งเตือนวิกฤต
- **New Columns (พบคอลัมน์ใหม่)**: สกัดชื่อคอลัมน์และสร้างข้อเสนอการเปลี่ยนผ่านโครงสร้างข้อมูล (Schema Drift Proposal)
- **การคำนวณระดับความรุนแรง (Drift Severity Weight)**:
  $$\text{Severity Score} = (\text{New Columns} \times 1) + (\text{Missing Columns} \times 5) + (\text{Type Mismatch} \times 5)$$
- **Evolution Gate Decision (การตัดสินใจผ่านสเปก)**:
  - **Safe Schema Drift** (พบเฉพาะคอลัมน์ใหม่เพิ่มเติม): ระบบจะผ่านให้อัตโนมัติ (**Auto-evolve**) โดยเขียนทับลงใน Elasticsearch Registry และปรับปรุงไฟล์ `schema_registry.json` บน Disk ให้ทันที บันทึก Proposal สถานะ `APPROVED`
  - **Dangerous Schema Drift** (คอลัมน์หายหรือชนิดข้อมูลเพี้ยน): ระบบจะทำการบล็อก (**Block Spec**) โดยจัดส่ง Proposal เข้าสู่ Elasticsearch สถานะ `PENDING` เพื่อรอคอยการกดยืนยันผ่าน API โดยวิศวกรข้อมูลด้วยตนเอง

### 3.5 Cleanse & Row-Level Validation (การตรวจสอบคุณภาพและคัดแยกข้อมูล)
ระบบทำการคัดแยกข้อมูลสะอาดออกจากข้อมูลชำรุด โดยตรวจเช็คกฎต่างๆ ในระดับแถวดังนี้:
1. **การตรวจสอบโครงสร้างพื้นฐาน**: แถวที่ไม่มี Primary Key หรือไม่มีคอลัมน์วันที่อัปเดต (Date Column is Null) จะถูกติดแถบ `is_invalid = True` ทันที ด้วยเหตุผล `missing_primary_key` หรือ `missing_date`
2. **การล้างข้อมูลซ้ำซ้อนอัจฉริยะ (Deduplication Logic)**:
   - นำเฉพาะเรคคอร์ดที่มี Primary Key ครบถ้วนมาทำ Deduplication
   - เรียงลำดับแถวตามวันเวลาอัปเดตล่าสุด (`date_column` Descending) จากนั้นเรียกใช้งาน `dropDuplicates` บนพาร์ทิชันเพื่อเลือกเฉพาะเรคคอร์ดล่าสุด (Latest Unique State)
   - แถวที่ซ้ำและมีช่วงเวลาเก่ากว่าจะถูกนำไปทำ **Anti-Join** ย้อนกลับเพื่อดึงออกมาแยกเก็บเข้า Quarantine Zone โดยทำเครื่องหมายเหตุผลว่า `duplicate_records`
3. **IQR Value Range Outlier Detection (การตรวจหาค่าหลุดขอบเขตด้วยสถิติ)**:
   - ระบบใช้สูตรการคำนวณช่วงการกระจายควอไทล์: $IQR = Q_3 - Q_1$
   - ขอบเขตข้อมูลปกติจะอยู่ระหว่าง: $[Q_1 - (1.5 \times IQR), Q_3 + (1.5 \times IQR)]$
   - แถวข้อมูลตัวเลขที่มีค่าหลุดพ้นขอบเขตบน/ล่าง (เช่น ยอดขายผิดปกติ) จะถูกคัดแยกออกไปยัง Quarantine ทันที
4. **Unsupervised Anomaly Detection (การหาความผิดปกติไร้ผู้ช่วยสอน)**:
   - คำนวณหาค่าเฉลี่ย ($\mu$) และส่วนเบี่ยงเบนมาตรฐาน ($\sigma$) ของคอลัมน์ตัวเลข
   - คำนวณค่า Z-score ของแต่ละแถว: $Z = \frac{x - \mu}{\sigma}$
   - หากแถวใดมี $|Z| > 3.0$ จะถือว่าเป็นความผิดปกติทางสถิติอย่างรุนแรง และส่งเข้ากักกัน
5. **Induced Tree Rules (กฎจากโมเดลจำลอง)**:
   - นำกฎเงื่อนไข SQL (เช่น `(price > 1000 AND category = 'Low')`) ที่เขียนขึ้นหรือได้มาจากการจำลองสถานการณ์ มากรองข้อมูล แถวใดที่เข้าข่ายผิดปกติจะถูกแยกไปที่ Quarantine

### 3.6 Atomic Write to HDFS (การเขียนข้อมูลแบบมีธุรกรรมความปลอดภัย)
- **Active Store (Silver Layer)**: นำข้อมูลสะอาดมาทำ **Delta Lake MERGE INTO** เข้าสู่ไดเรกทอรีปลายทาง โดยเทียบเงื่อนไขคีย์หลักเพื่อทำคำสั่งอัปเดต/แทรกแถวใหม่ (Upsert) แบบอะตอม (Atomic Transaction) 
- หากตารางนั้นเพิ่งเขียนครั้งแรก ระบบจะทำการเปิดสิทธิ์ Column Mapping (`delta.columnMapping.mode = name`) และกำหนดคุณสมบัติ Transaction version เพื่อให้รองรับ Schema Evolution ในอนาคต
- **Quarantine Zone**: นำข้อมูลที่เสียหายทั้งหมดมาเขียนต่อท้าย (Append) ลงใน HDFS Quarantine แยกตามโฟลเดอร์พาร์ทิชัน `run_id` เพื่อความสะดวกในการวิเคราะห์และล้างข้อมูลใหม่

---

## 4. รายละเอียดการคำนวณตัวชี้วัด (Metrics Processing Details)

เมื่อเสร็จสิ้นการคัดแยก Spark จะคำนวณข้อมูลสรุปเพื่อบันทึกลง Elasticsearch:

### 4.1 Data Quality Score (คะแนนคุณภาพข้อมูล)
คำนวณเป็นร้อยละของข้อมูลที่ผ่านเกณฑ์คุณภาพโดยตรงจากสูตร:
$$\text{Quality Score} = \left( \frac{\text{Clean Records (Active)}}{\text{Total Records Ingested}} \right) \times 100$$
*หมายเหตุ: หากข้อมูลนำเข้าเป็นไฟล์เปล่า (0 แถว) ระบบจะคิดคะแนนเป็น 0.0% เพื่อชี้วัดให้เห็นถึงความผิดพลาดในกระบวนการ Ingestion ทันที*

### 4.2 Dynamic Financial COPDQ (มูลค่าความสูญเสียจากข้อมูลเสีย)
**Cost of Poor Data Quality (COPDQ)** คำนวณแบบพลวัต (Dynamic) โดยสแกนหาคอลัมน์ที่เกี่ยวกับยอดเงิน เช่น `sales`, `price`, `amount` ที่อยู่ในกลุ่มแถวข้อมูลที่ถูกกักกัน (Quarantined) แล้วทำการรวมมูลค่า (Sum) เพื่อแสดงให้เห็นถึงความเสียหายเชิงมูลค่าการเงินจริงที่เกิดขึ้นจากการประมวลผลข้อมูลไม่ได้คุณภาพ

### 4.3 Z-Score Anomaly on Quarantine Rate (อัตราข้อมูลเสียผิดปกติสะสม)
ระบบดึงอัตราส่วนการคัดแยกข้อมูลดิบ (Quarantine Rate) ย้อนหลัง 15 รอบการทำงาน เพื่อนำมาคำนวณหาค่าผิดปกติ:
- ใช้สูตร Standard Deviation และ Mean ของประวัติเก่าเพื่อคำนวณ Z-score
- ตั้งค่าขอบเขตความคลาดเคลื่อนขั้นต่ำ ($\sigma \text{ floor} = 0.05$) เพื่อป้องกันไม่ให้อัตราความแปรปรวนน้อยๆ ทำให้ผลลัพธ์คลาดเคลื่อน และตั้งกฎให้ระงับการแจ้งเตือนหากสัดส่วนที่แกว่งตัวต่างกันไม่เกิน 2%

---

## 5. สถาปัตยกรรมชั้นวิเคราะห์สรุปผล (Gold Layer Aggregation Logic)

เพื่อหลีกเลี่ยงไม่ให้การดึงข้อมูลแดชบอร์ดหรือ BI Tools ต้องไปรันคิวรีสรุปผลกับระบบดิบที่มีข้อมูลขนาดใหญ่ซ้ำๆ โครงสร้างของ SDOQAP ได้พัฒนาส่วนงาน **Gold Layer Aggregation Engine** (`spark_gold_layer.py`) ซึ่งทำงานดังนี้:

1. **สกัดผลลัพธ์รายวัน (Daily Compaction)**: อ่านข้อมูลสะสมจาก Silver Layer (`/data/active`) และผลการทำงานในดัชนี ES มาบดสรุปข้อมูลรายชั่วโมง/รายวัน
2. **การ Pre-aggregate**: จัดเก็บผลลัพธ์แยกออกเป็นดัชนีพร้อมใช้งานสำหรับสถิติรายวัน สัดส่วนประเภทของข้อผิดพลาดที่จำแนกได้ และกราฟ COPDQ
3. ** serving layer**: FastAPI จะดึงผลลัพธ์จาก Gold index เหล่านี้ตอบสนองกลับไปยัง React Portal ทำให้หน้าจอแดชบอร์ดโหลดข้อมูลเสร็จสิ้นในระยะเวลาเพียงเสี้ยววินาที

---

## 6. บริการด้านการตรวจสอบความปลอดภัยของข้อมูล (Trust-Check API & Gate)

การป้องกันไม่ให้ระบบปลายน้ำ (BI/ML) ดึงข้อมูลไม่ได้มาตรฐานไปวิเคราะห์ เป็นสิ่งที่สำคัญตามนโยบาย Upstream-First:
- **Trust-Check API (`GET /api/v1/lineage/{table_name}/trust-check`)**: ระบบจะคำนวณคะแนนล่าสุด ตรวจสอบระดับเปรียบเทียบกับ Threshold ของตาราง และตรวจสอบว่ามี Proposal ที่ติดค้างการอนุมัติ (Pending Proposal) หรือไม่
- หากคะแนนอยู่ในเกณฑ์และไม่มี Proposal ค้างอยู่ ระบบจะคืนสถานะ `recommendation: "SAFE"` และ `is_safe_to_consume: true`
- หากไม่ตรงตามเงื่อนไข ระบบจะแนะนำให้ปฏิเสธการดึงไปใช้งานปลายทาง เพื่อสกัดกั้นการใช้ข้อมูลที่ชำรุด

---

## 7. ปรัชญา Upstream-First Remediation (กระบวนการจัดการปัญหาที่ต้นเหตุ)

ตามแนวคิดการแก้ไขปัญหาที่ต้นน้ำ โฟลเดอร์กักกัน (Quarantine Store) มีหน้าที่ปกป้องระบบหลัก แต่การแก้ปัญหาที่ถาวรคือ **"การปรับปรุงข้อมูลที่ต้นทาง (Upstream)"** โดยระบบรองรับวงรอบปฏิบัติดังนี้:

```
[ Spark ตรวจพบปัญหา ]
         │
         ▼
[ สร้าง Upstream Remediation Ticket ใน ES ]
         │
         ▼
[ แจ้งเตือนวิศวกรข้อมูล / ผู้ดูแล API/Database ต้นทาง ]
         │
         ▼
[ ปรับปรุงโค้ด API หรือปรับเปลี่ยน Schema ที่ต้นน้ำ ]
         │
         ▼
[ วิศวกรข้อมูลอนุมัติ / ปิด Ticket และสั่ง Retry Ingestion ]
```

### ตัวอย่างการประยุกต์ใช้งาน
- **Schema Drift (API เพิ่มฟิลด์ใหม่)**: หากตรวจพบว่าเป็นการเพิ่มฟิลด์แบบปลอดภัย ข้อมูลจะถูกอนุมัติลง registry ทันที แต่ถ้าพบคอลัมน์สำคัญหายไป ระบบจะบล็อกและสร้างตั๋วแก้ไขปัญหา (Remediation Ticket) ระบบจะแนะนำข้อกำหนดใหม่และแจ้งกลับผู้ดูแล API ต้นทางให้แก้ไของค์ประกอบข้อมูล เมื่อแก้ไขแล้วจึงกดยืนยันเพื่อนำเข้าชุดข้อมูลใหม่อีกครั้ง
- **ข้อมูลผิดรูปแบบ (เช่น ฟิลด์เบอร์โทรศัพท์มีตัวหนังสือ)**: Spark จะกักกันแถวนั้นและบันทึกประวัติข้อบกพร่องสะสม ระบบ FastAPI จะสร้างข้อแนะนำให้ปรับปรุง Logic การทำความสะอาดข้อมูลที่ระบบ Ingest ดั้งเดิม เพื่อแก้ปัญหาไม่ให้มีข้อมูลปนเปื้อนหลุดเข้ามาในท่อส่งข้อมูลหลักอีกในอนาคต
