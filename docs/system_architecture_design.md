# เอกสารการออกแบบสถาปัตยกรรมระบบ (System Architecture Design)
## โครงการ SDOQAP (Scalable Data Observability and Quality Assurance Platform)

เอกสารนี้แสดงการออกแบบโครงสร้างสถาปัตยกรรมการทำงานเชิงลึกของระบบ SDOQAP ตั้งแต่ต้นน้ำจนถึงปลายน้ำ โดยครอบคลุมทั้งการคัดแยกข้อมูลคุณภาพ และระบบควบคุมธรรมาภิบาลโครงสร้างข้อมูลที่เปลี่ยนแปลง (Schema Drift Governance)

---

## 1. แผนภาพสถาปัตยกรรมภาพรวม (High-Level Architecture Diagram)
แผนภาพนี้แสดงโครงสร้างสถาปัตยกรรมข้อมูลแบบ **Medallion Architecture** โดยแบ่งโซนข้อมูลออกเป็น **Bronze, Silver, และ Gold Layers** เพื่อแสดงการไหลของข้อมูลตั้งแต่ต้นทางจนออกไปยังแผงควบคุมและช่องบริการข้อมูล

```mermaid
graph TD
    %% ==========================================
    %% CLASS DEFINITIONS (draw.io Pastel Styles)
    %% ==========================================
    classDef source fill:#f8f9fa,stroke:#343a40,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef bronze fill:#eff6ff,stroke:#2563eb,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef silver fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef gold fill:#fffbeb,stroke:#d97706,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef engine fill:#faf5ff,stroke:#7c3aed,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef ui fill:#fdf2f8,stroke:#db2777,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;

    subgraph Data_Sources ["แหล่งข้อมูลนำเข้า (Data Ingestion)"]
        CSV["📄 Local CSV Files<br>(ชุดข้อมูลฝั่งโฮสต์)"]:::source
        API["🌐 REST API URLs<br>(ระบบ API ภายนอก)"]:::source
        Reddit["🤖 Reddit Streamer<br>(ดึงข้อมูลสดผ่าน API)"]:::source
    end

    subgraph Bronze_Layer ["1. Bronze Layer (Raw Storage)"]
        HDFS_Bronze["📁 HDFS Raw Zone (Bronze)<br>(/data/raw/&lt;table&gt;/)<br>เก็บข้อมูลดิบเป็นไฟล์ตามสภาพจริง"]:::bronze
    end

    subgraph Compute_Engine ["เครื่องประมวลผลหลัก (Processing Cluster)"]
        Spark["⚡ Apache Spark Engine Cluster<br>(sdoqap-spark-master / worker)<br>ประมวลผลสเกลขนานตามกฎและเปรียบเทียบสเปก"]:::engine
    end

    subgraph Silver_Layer ["2. Silver Layer (Cleaned & Quarantined Data)"]
        HDFS_Silver["🟢 Delta Lake Active Zone (Silver)<br>(/data/active/&lt;table&gt;/)<br>เก็บข้อมูลสะอาดที่ผ่านเกณฑ์พร้อมใช้ รองรับ ACID"]:::silver
        HDFS_Quar["🔴 HDFS Quarantine Zone (Silver)<br>(/data/quarantine/&lt;table&gt;/)<br>กักแยกแถวพังระบุรหัสงานและสาเหตุที่ตกเกณฑ์"]:::silver
    end

    subgraph Gold_Layer ["3. Gold Layer (Business Aggregations)"]
        ES_Gold["📊 Elasticsearch Indices (Gold)<br>(ข้อมูล pre-aggregate คุณภาพ, Lineage,<br>และสถิติสะสมเพื่อลด shuffle overhead)"]:::gold
    end

    subgraph Outputs ["4. การให้บริการและการสังเกตการณ์ (Serving & Observability)"]
        API_Serv["⚡ FastAPI Serving Layers<br>(บริการ REST API & Trust-Check API)"]:::ui
        Grafana["📊 Grafana dashboards<br>(สรุปแดชบอร์ดสถิติ ความเสียหายเชิงการเงิน COPDQ)"]:::ui
        Portal["💻 Central Portal UI<br>(React Dashboard / หน้าอนุมัติ Schema)"]:::ui
    end

    %% Flow lines
    CSV -->| batch_upload คัดลอกผ่าน NameNode | HDFS_Bronze
    API -->| ดาวน์โหลดและแปลง CSV | HDFS_Bronze
    Reddit -->| สตรีมสดผ่าน Kafka Queue | Spark
    
    HDFS_Bronze -->| อ่านข้อมูลดิบไปล้างและกรอง | Spark
    
    Spark -->| เขียนข้อมูลผ่านเกณฑ์ด้วยคำสั่ง MERGE | HDFS_Silver
    Spark -->| เขียนต่อท้ายข้อมูลเสียแยกโฟลเดอร์ | HDFS_Quar
    
    HDFS_Silver -->| ประมวลผลลอจิกสรุปผล Gold Layer | ES_Gold
    
    ES_Gold -->| แสดงผลและค้นหา | API_Serv
    ES_Gold -->| พล็อตแดชบอร์ดกราฟคุณภาพ | Grafana
    
    API_Serv -->| เชื่อมต่อเพื่อควบคุมและดึงข้อมูล | Portal
```

---

## 2. แผนภาพการคัดแยกข้อมูลระดับแถว (Data Segregation Flow)
แผนภาพนี้แสดงตรรกะหัวใจสำคัญของโครงการ (Data Segregation) โดยแสดงให้เห็นว่าเมื่อข้อมูลเข้าสู่ Spark แล้ว ข้อมูลที่ดีจะไหลไปที่ตารางสะอาด Active (Silver) อย่างไร และข้อมูลที่ชำรุดรายบรรทัดจะถูกคัดแยกแยกแยะส่งไป Quarantine Store โดยที่ระบบประมวลผลรวม (Pipeline) ไม่ล่มหยุดชะงัก

```mermaid
graph TD
    classDef step fill:#f8f9fa,stroke:#4b5563,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;
    classDef decision fill:#fffbeb,stroke:#d97706,stroke-width:1.5px,color:#212529;
    classDef active fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,rx:5px,ry:5px,color:#212529;
    classDef quarantine fill:#fef2f2,stroke:#dc2626,stroke-width:2px,rx:5px,ry:5px,color:#212529;
    classDef log fill:#faf5ff,stroke:#7c3aed,stroke-width:1.5px,rx:5px,ry:5px,color:#212529;

    Start["📥 ดึงข้อมูลดิบจาก HDFS Raw"]:::step --> LockCheck{"🔒 ตรวจสอบ Distributed Lock ใน Elasticsearch"}:::decision
    
    LockCheck -->| ชนสิทธิ์ / มีงานกำลังรันค้างอยู่ | Block["🛑 ยกเลิกรัน (Self-Healing / Lock Expire 15 นาที)"]:::quarantine
    LockCheck -->| ได้รับสิทธิ์ล็อก | ReadData["📖 อ่านข้อมูลและปรับพาร์ทิชันตามขนาด (Shuffle Partition Tuning)"]:::step
    
    ReadData --> ParseCheck{"🔑 มีคีย์หลัก (Primary Key)<br>& วันที่ (Date Column)?"}:::decision
    ParseCheck -->| ไม่มี / เป็นค่าว่าง | Quar_PK["🔴 คัดแยกไปยัง Quarantine Zone<br>(missing_primary_key / missing_date)"]:::quarantine
    
    ParseCheck -->| มีข้อมูลครบถ้วน | Deduplicate["✨ ล้างข้อมูลซ้ำซ้อนย้อนหลัง (Deduplication)<br>จัดเรียงคัดเฉพาะข้อมูลใหม่ที่สุดในชุดนั้น"]:::step
    
    Deduplicate --> DupCheck{"⌛ มีข้อมูลซ้ำที่อัปเดตเก่ากว่าใน Silver Layer?"}:::decision
    DupCheck -->| มี | Quar_Dup["🔴 คัดแยกไปยัง Quarantine Zone<br>(duplicate_records)"]:::quarantine
    
    DupCheck -->| ไม่มี | Standardize["⚙️ ปรับแต่งชื่อคอลัมน์มาตรฐาน (Standardization)<br>& เลื่อนชนิดข้อมูลอย่างปลอดภัย (Safe Type Promotion)"]:::step
    
    Standardize --> ValueCheck{"📏 ตรวจสอบค่าผิดปกติด้วยสถิติ<br>(IQR Outliers & Z-Score Anomaly)"}:::decision
    ValueCheck -->| ค่าแกว่งหลุดเกณฑ์ปกติเกิน 3 เท่า | Quar_Val["🔴 คัดแยกไปยัง Quarantine Zone<br>(iqr_outlier / statistical_anomaly)"]:::quarantine
    
    ValueCheck -->| ผ่านสถิติ | RulesCheck{"📋 ตรวจสอบกฎเงื่อนไขธุรกิจในตาราง<br>(Induced Tree Rules)"}:::decision
    RulesCheck -->| ขัดแย้งกับกฎคุณภาพ | Quar_Rules["🔴 คัดแยกไปยัง Quarantine Zone<br>(induced_rule_failure)"]:::quarantine
    
    RulesCheck -->| ผ่านเกณฑ์ทั้งหมด | ActiveStore["🟢 เขียนเข้า Silver Active Store (Delta Lake)<br>ด้วยคำสั่ง MERGE INTO แบบ Upsert"]:::active
    
    %% Output Aggregation & Metadata logging
    ActiveStore --> Summary["📊 คำนวณร้อยละคะแนน Data Quality Score & COPDQ"]:::step
    Quar_PK --> CombineQuar["🛠️ รวบรวมแถวชำรุดทั้งหมดเขียนต่อท้ายลง HDFS กักกันแยกตาม run_id"]:::quarantine
    Quar_Dup --> CombineQuar
    Quar_Val --> CombineQuar
    Quar_Rules --> CombineQuar
    
    CombineQuar --> Summary
    Summary --> LogES["📝 บันทึกข้อมูลและสถิติทั้งหมดส่งไปยัง Elasticsearch<br>(sdoqap_quality_runs & sdoqap_pipeline_runs)"]:::log
    LogES --> End["🏁 งานประมวลผลเสร็จสิ้น (Pipeline รันจบได้สมบูรณ์)"]:::step
```

---

## 3. กลไกจัดการโครงสร้างเปลี่ยนรูป (Schema Drift Governance Flow)
แผนภาพนี้แสดงผังการตัดสินใจเมื่อพบคอลัมน์มีการเพิ่มขึ้น หายไป หรือเปลี่ยนชนิดข้อมูลอย่างไม่สอดคล้องกับไฟล์สเปก `schema_registry.json` เพื่อทำระบบควบคุมอนุมัติแบบกึ่งอัตโนมัติ (Schema Approval Gate)

```mermaid
graph TD
    classDef start fill:#f8f9fa,stroke:#4b5563,stroke-width:1.5px,color:#212529;
    classDef process fill:#e8f4fd,stroke:#1d78c1,stroke-width:1.5px,color:#212529;
    classDef decision fill:#fffbeb,stroke:#d97706,stroke-width:1.5px,color:#212529;
    classDef auto fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,color:#212529;
    classDef block fill:#fef2f2,stroke:#dc2626,stroke-width:2px,color:#212529;
    classDef user fill:#faf5ff,stroke:#7c3aed,stroke-width:2px,color:#212529;

    Start["📥 Spark ตรวจสอบโครงสร้างข้อมูลนำเข้า"]:::start --> Compare{"🔍 เปรียบเทียบกับ Schema ดั้งเดิม<br>(schema_registry.json)"}:::process
    
    Compare --> CheckDrift{"⚠️ ตรวจพบ Schema Drift หรือไม่?"}:::decision
    
    CheckDrift -->| ไม่พบ | ContinueBatch["🟢 ประมวลผลข้อมูลต่อ เขียนลง HDFS Active"]:::auto
    
    CheckDrift -->| ตรวจพบ | CalcSeverity["📏 คำนวณคะแนนความรุนแรงของโครงสร้าง<br>(Severity Score = New*1 + Missing*5 + TypeMismatch*5)"]:::process
    
    CalcSeverity --> EvalGate{"🚦 ประเมินขีดเกณฑ์ผ่านสเปก (Evolution Gate)"}:::decision
    
    EvalGate -->| ปลอดภัย (พบคอลัมน์ใหม่เพิ่มเติมเท่านั้น Score <= 4) | AutoApprove["🟢 Auto-Evolve (อนุมัติผ่านสเปกอัตโนมัติ)<br>- อัปเดตสเปกบน Disk และ ES ทันที<br>- บันทึก Proposal สถานะ APPROVED ใน ES"]:::auto
    AutoApprove --> ContinueBatch
    
    EvalGate -->| อันตราย (คอลัมน์หายหรือชนิดข้อมูลเพี้ยน Score > 4) | BlockSpec["🔴 Block Spec (ระงับโครงสร้างแบบมีเกตควบคุม)<br>- แปลงคอลัมน์เป็น String เพื่อรักษาการรันภาพรวม<br>- ส่งข้อเสนอเป็นสถานะ PENDING ใน ES<br>- แจ้งเตือน Critical Alert ไป n8n เพื่อบอกวิศวกรข้อมูล"]:::block
    
    BlockSpec --> AlertNotify["📢 n8n ยิงสตรีมแจ้งเตือนไปยังทีม Data Engineer (Slack/Line/Teams)"]:::process
    
    AlertNotify --> DE_Intervene["👨‍💻 Data Engineer เข้าตรวจสอบรายละเอียดผ่าน Central Portal UI"]:::user
    
    DE_Intervene --> Decision{"ตัดสินใจอนุมัติโครงสร้างนี้หรือไม่?"}:::decision
    
    Decision -->| อนุมัติ (Approve via API) | Approved["🟢 บันทึกยืนยันโครงสร้างใหม่<br>- อัปเดตข้อมูลไฟล์สเปกบน Disk<br>- ปรับสถานะ Proposal ใน ES เป็น APPROVED"]:::auto
    Decision -->| ปฏิเสธ (Reject via API) | Rejected["🔴 บันทึกการปฏิเสธโครงสร้าง<br>- ปรับสถานะเป็น REJECTED<br>- ข้อมูลรอบถัดไปที่เพี้ยนจะเข้าโฟลเดอร์กักกัน"]:::block
    
    Approved --> NextRun["🔄 ข้อมูลที่ค้างอยู่จะถูกดึงเข้าประมวลผลต่อตามโครงสร้างใหม่ในรอบถัดไป"]:::process
```

---

## 4. แผนผังเทคโนโลยีและการควบคุมอัตโนมัติ (Technology Stack & Automation Map)

ระบบนี้รวมการทำงานเข้าด้วยกันเป็นสแต็กเทคโนโลยีที่มีการไหลและการสั่งการทำงานที่เป็นอัตโนมัติ (Automated Pipelines):

| บทบาทการทำงาน (System Role) | เทคโนโลยีหลัก (Tech Stack) | ลักษณะการควบคุมอัตโนมัติ (Automation Features) |
| :--- | :--- | :--- |
| **Ingestion Engine** (นำเข้าข้อมูล) | Python, PowerShell scripts, **Apache Kafka**, **Zookeeper** | ทริกเกอร์ตามคาบเวลา ดาวน์โหลด API ตรวจหา payload แปลง JSON และสตรีมสดเข้าคิวรับส่งทันที |
| **Data Lake Storage** (การเก็บข้อมูลหลัก) | **Apache Hadoop HDFS**, **Delta Lake** | จัดเก็บเลเยอร์แบบ Medallion โดยแยกสิทธิ์เขียนอัตโนมัติ ข้อมูลสะอาดเขียนผ่าน MERGE (Upsert) ป้องกันข้อมูลซ้ำ |
| **Compute Engine** (ตัวประมวลผลหลัก) | **Apache Spark** (Master-Worker Cluster) | ประมวลผลแบบกระจายศูนย์อัตโนมัติ ปรับ Shuffling อัตโนมัติตามขนาดข้อมูล คัดแยกแถวเสียลง Quarantine Zone แบบ Row-level |
| **Observability DB** (ระบบตรวจสอบ) | **Elasticsearch**, **Kibana** | ดักจับสถานะ จัดการ distributed locks อัตโนมัติ ปลดปล่อยสิทธิ์การล็อกแบบเซฟตี้ เก็บดัชนีคุณภาพและ Lineage |
| **Serving Layer** (ส่วนคุมและให้บริการ) | **FastAPI**, **Nginx** | บริหารจัดการการเข้าถึง API, บริการฟังก์ชัน **Trust-Check** สำหรับดึงข้อมูลปลายทาง และเป็นตัวกลางอนุมัติ Schema Drift |
| **UI Portal & Monitoring** (หน้าแผงคุม) | **React.js (Central Portal)**, **Grafana** | แสดงสถิติและ Lineage อัตโนมัติ วาดกราฟเปรียบเทียบคุณภาพและแสดงค่า COPDQ (มูลค่าความสูญเสียจากข้อมูลชำรุด) |
| **Workflow Automation** (ท่อควบคุมภายนอก) | **n8n** | ตรวจสอบดัชนีใน ES คอยแจ้งเตือนภัยผ่านช่องแชทแบบเรียลไทม์ และประสานงานส่งต่อ API ทันทีเมื่อเกิด Schema Drift |
