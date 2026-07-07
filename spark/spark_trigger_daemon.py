import os
import sys
import subprocess
import json
import time
import threading
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler

# Streaming globals
active_stream_proc = None
active_spark_proc = None
stream_thread = None
stream_logs = []
stream_start_time = None
stream_duration = 0
stream_subreddits = ""
stream_status = "idle"  # "idle" or "running"
stream_lock = threading.Lock()

def append_log(msg):
    stream_logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")
    if len(stream_logs) > 500:
        stream_logs.pop(0)

def run_stream_job(subreddits, duration):
    global active_stream_proc, active_spark_proc, stream_status, stream_start_time, stream_duration, stream_subreddits
    with stream_lock:
        stream_status = "running"
        stream_start_time = time.time()
        stream_duration = duration
        stream_subreddits = subreddits
        stream_logs.clear()
        append_log(f"[SYSTEM] Starting stream job with subreddits: {subreddits} for {duration} seconds")

    try:
        # Start reddit_stream.py
        append_log("[INGEST] Starting background Reddit stream ingestion...")
        ingest_cmd = ["python", "-u", "/opt/spark-apps/reddit_stream.py", subreddits]
        active_stream_proc = subprocess.Popen(
            ingest_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Start streaming_job.py
        append_log("[SPARK] Starting background Spark streaming job...")
        spark_cmd = [
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/spark-apps/streaming_job.py"
        ]
        active_spark_proc = subprocess.Popen(
            spark_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Helper to read output logs in background
        def read_output(proc, prefix):
            try:
                for line in iter(proc.stdout.readline, ""):
                    if line:
                        append_log(f"{prefix} {line.strip()}")
            except Exception as e:
                append_log(f"[ERROR] Error reading {prefix} logs: {e}")
            finally:
                try:
                    proc.stdout.close()
                except Exception:
                    pass
            
        t1 = threading.Thread(target=read_output, args=(active_stream_proc, "[python]"))
        t2 = threading.Thread(target=read_output, args=(active_spark_proc, "[spark]"))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()

        # Wait for duration or manual stop
        end_time = stream_start_time + duration
        while time.time() < end_time:
            with stream_lock:
                if stream_status != "running":
                    break
            time.sleep(0.5)
            
    except Exception as e:
        append_log(f"[ERROR] Stream job failed: {e}\n{traceback.format_exc()}")
    finally:
        # Terminate
        append_log("[SYSTEM] Terminating streaming jobs...")
        if active_stream_proc:
            try:
                active_stream_proc.terminate()
                active_stream_proc.wait(timeout=2)
            except Exception:
                try:
                    active_stream_proc.kill()
                except Exception:
                    pass
        if active_spark_proc:
            try:
                active_spark_proc.terminate()
                active_spark_proc.wait(timeout=2)
            except Exception:
                try:
                    active_spark_proc.kill()
                except Exception:
                    pass
        
        with stream_lock:
            stream_status = "idle"
            active_stream_proc = None
            active_spark_proc = None
        append_log("[SYSTEM] Streaming pipeline stopped.")

class SparkTriggerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/stream/status":
            with stream_lock:
                elapsed = 0
                remaining = 0
                if stream_status == "running" and stream_start_time:
                    elapsed = int(time.time() - stream_start_time)
                    remaining = max(0, stream_duration - elapsed)
                response = {
                    "status": stream_status,
                    "subreddits": stream_subreddits,
                    "duration": stream_duration,
                    "elapsed": elapsed,
                    "remaining": remaining,
                    "logs": list(stream_logs)
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global stream_thread, stream_status, stream_start_time, stream_duration
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
                
                with stream_lock:
                    stream_logs.clear()
                    append_log(f"[SYSTEM] Starting Spark Quality Engine Rerun for table '{table_name}'")
                    stream_status = "running"
                    stream_start_time = time.time()
                    stream_duration = 300
                
                def run_rerun_job(cmd_args, tbl):
                    global stream_status
                    try:
                        proc = subprocess.Popen(
                            cmd_args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            bufsize=1
                        )
                        for line in iter(proc.stdout.readline, ""):
                            if line:
                                append_log(f"[spark] {line.strip()}")
                        proc.wait()
                        append_log(f"[SYSTEM] Spark Quality Engine finished for table '{tbl}' (Exit code: {proc.returncode})")
                    except Exception as e:
                        append_log(f"[ERROR] Spark Quality Engine run failed: {e}")
                    finally:
                        with stream_lock:
                            stream_status = "idle"
                
                t = threading.Thread(target=run_rerun_job, args=(cmd, table_name))
                t.daemon = True
                t.start()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "success", "message": f"Spark job triggered for table '{table_name}'"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error triggering job: {e}".encode("utf-8"))
                
        elif self.path == "/gold/rebuild":
            try:
                print("[DAEMON] Triggering Gold Layer rebuild...")
                cmd = ["python", "/opt/spark-apps/spark_gold_layer.py"]
                
                with stream_lock:
                    stream_logs.clear()
                    append_log("[SYSTEM] Starting Gold Layer rebuild...")
                    stream_status = "running"
                    stream_start_time = time.time()
                    stream_duration = 300
                
                def run_rebuild_job(cmd_args):
                    global stream_status
                    try:
                        proc = subprocess.Popen(
                            cmd_args,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            bufsize=1
                        )
                        for line in iter(proc.stdout.readline, ""):
                            if line:
                                append_log(f"[python] {line.strip()}")
                        proc.wait()
                        append_log(f"[SYSTEM] Gold Layer rebuild finished (Exit code: {proc.returncode})")
                    except Exception as e:
                        append_log(f"[ERROR] Gold Layer rebuild failed: {e}")
                    finally:
                        with stream_lock:
                            stream_status = "idle"
                
                t = threading.Thread(target=run_rebuild_job, args=(cmd,))
                t.daemon = True
                t.start()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "success", "message": "Gold Layer rebuild triggered"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error triggering Gold Layer rebuild: {e}".encode("utf-8"))
                
        elif self.path == "/stream/start":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                data = json.loads(body)
                subreddits = data.get("subreddits", "python")
                duration = int(data.get("duration", 40))
                
                with stream_lock:
                    if stream_status == "running":
                        self.send_response(400)
                        self.end_headers()
                        self.wfile.write(b"Streaming job already running")
                        return
                    
                stream_thread = threading.Thread(target=run_stream_job, args=(subreddits, duration))
                stream_thread.daemon = True
                stream_thread.start()
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"status": "success", "message": "Streaming job successfully started"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Error starting streaming job: {e}".encode("utf-8"))
                
        elif self.path == "/stream/stop":
            with stream_lock:
                if stream_status != "running":
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"No active streaming job to stop")
                    return
                stream_status = "stopping"
                
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "success", "message": "Streaming job termination requested"}
            self.wfile.write(json.dumps(response).encode("utf-8"))
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
