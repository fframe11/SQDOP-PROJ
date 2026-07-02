from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta

# Simple sensor that checks Kafka consumer lag > 0
import socket
import json
import re

# Simple sensor that checks Kafka consumer lag > 0
class KafkaLagSensor(BaseSensorOperator):
    """Poll Kafka consumer group for pending messages."""
    def poke(self, context):
        socket_path = "/var/run/docker.sock"
        payload = {
            "AttachStdout": True,
            "AttachStderr": False,
            "Tty": False,
            "Cmd": ["kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092", "--describe", "--group", "reddit-consumer"]
        }
        
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect(socket_path)
            body = json.dumps(payload)
            req = (
                "POST /containers/kafka/exec HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n\r\n"
                f"{body}"
            )
            s.sendall(req.encode('utf-8'))
            response = s.recv(4096).decode('utf-8', errors='ignore')
            s.close()
            
            m = re.search(r'\{.*\}', response, re.DOTALL)
            if not m:
                return False
            exec_id = json.loads(m.group(0)).get("Id")
            if not exec_id:
                return False
                
            s_start = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s_start.connect(socket_path)
            start_payload = {"Detach": False, "Tty": False}
            start_body = json.dumps(start_payload)
            start_req = (
                f"POST /exec/{exec_id}/start HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(start_body)}\r\n\r\n"
                f"{start_body}"
            )
            s_start.sendall(start_req.encode('utf-8'))
            
            output = ""
            while True:
                chunk = s_start.recv(4096).decode('utf-8', errors='ignore')
                if not chunk:
                    break
                output += chunk
            s_start.close()
            
            for line in output.splitlines():
                if "reddit_raw" in line:
                    parts = line.split()
                    if len(parts) >= 6:
                        lag = int(parts[5])
                        print(f"[SENSOR] Real-time Kafka consumer lag: {lag}")
                        return lag > 0
        except Exception as e:
            print(f"[SENSOR] Failed to fetch Kafka lag via socket: {e}")
        return False

default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_stream_pipeline",
    default_args=default_args,
    description="Ingest Reddit → Kafka → Spark → HDFS/ES",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-eng", "streaming"],
) as dag:

    kafka_check = DockerOperator(
        task_id="kafka_check",
        image="confluentinc/cp-kafka:7.5.0",
        command="bash -c \"kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group reddit-consumer\"",
        docker_url="unix://var/run/docker.sock",
        network_mode="sdoqap_network",
        auto_remove=True,
        retrieve_output=True,
    )

    wait_for_messages = KafkaLagSensor(task_id="wait_for_messages", poke_interval=30, timeout=300)

    spark_submit = DockerOperator(
        task_id="spark_submit",
        image="dataengproj-spark-worker:latest",
        command=(
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2 "
            "/opt/spark-apps/streaming_job.py"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="sdoqap_network",
        auto_remove=True,
    )

    kafka_check >> wait_for_messages >> spark_submit
