from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta

# Simple sensor that checks Kafka consumer lag > 0
class KafkaLagSensor(BaseSensorOperator):
    """Poll Kafka consumer group for pending messages."""
    def poke(self, context):
        cmd = (
            "kafka-consumer-groups.sh --bootstrap-server kafka:9092 "
            "--describe --group reddit-consumer | grep reddit_raw | awk '{print $6}'"
        )
        result = context["ti"].xcom_pull(task_ids="kafka_check")
        try:
            lag = int(result.strip())
        except Exception:
            lag = 0
        return lag > 0

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
        network_mode="dataeng",
        auto_remove=True,
    )

    wait_for_messages = KafkaLagSensor(task_id="wait_for_messages", poke_interval=30, timeout=300)

    spark_submit = DockerOperator(
        task_id="spark_submit",
        image="bitnami/spark:3.5.0",
        command="/opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/app/streaming_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="dataeng",
        auto_remove=True,
    )

    kafka_check >> wait_for_messages >> spark_submit
