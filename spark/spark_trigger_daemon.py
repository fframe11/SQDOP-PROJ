import os
import sys
import subprocess
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

class SparkTriggerHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/retry":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                data = json.loads(body)
                table_name = data.get("table")
                if not table_name:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Missing 'table' in payload")
                    return

                print(f"[DAEMON] Triggering Spark rerun for table: {table_name}")
                
                # Command to run spark-submit inside the container
                cmd = [
                    "spark-submit",
                    "--master", "spark://spark-master:7077",
                    "--conf", "spark.executorEnv.HADOOP_USER_NAME=spark",
                    "--conf", "spark.executor.extraJavaOptions=-DHADOOP_USER_NAME=spark",
                    "--conf", "spark.driver.extraJavaOptions=-DHADOOP_USER_NAME=spark",
                    "--packages", "io.delta:delta-core_2.12:2.4.0",
                    "/opt/spark-apps/spark_quality_engine.py",
                    table_name
                ]
                
                # Execute asynchronously
                subprocess.Popen(cmd)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "success", "message": f"Spark job triggered for table '{table_name}'"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error triggering job: {e}".encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

def run(port=8099):
    server_address = ('', port)
    httpd = HTTPServer(server_address, SparkTriggerHandler)
    print(f"[DAEMON] Spark Trigger Daemon listening on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == "__main__":
    port = int(os.getenv("SPARK_TRIGGER_PORT", 8099))
    run(port=port)
