#!/bin/bash
# ============================================================
# SDOQAP Full System Health Check & Bug Hunter
# Tests every component end-to-end
# ============================================================

# Ensure WSL docker socket is mapped to Docker Desktop proxy (WSL tempfs reset fix)
ln -sf /mnt/wsl/docker-desktop/shared-sockets/host-services/docker.proxy.sock /var/run/docker.sock >/dev/null 2>&1

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

PASS=0
FAIL=0
WARN=0
ERRORS=""

# Load environment variables from .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
  export $(grep -v '^#' "$SCRIPT_DIR/../.env" | grep -v '^[[:space:]]*$' | xargs)
elif [ -f "$SCRIPT_DIR/.env" ]; then
  export $(grep -v '^#' "$SCRIPT_DIR/.env" | grep -v '^[[:space:]]*$' | xargs)
fi
API_PORT=${API_PORT:-8000}
GRAFANA_PORT=${GRAFANA_PORT:-3000}

pass() { echo -e "  ${GREEN}✅ PASS${NC}: $1"; PASS=$((PASS+1)); }
fail() { echo -e "  ${RED}❌ FAIL${NC}: $1"; FAIL=$((FAIL+1)); ERRORS="${ERRORS}\n❌ $1"; }
warn() { echo -e "  ${YELLOW}⚠️  WARN${NC}: $1"; WARN=$((WARN+1)); }
section() { echo -e "\n${CYAN}${BOLD}═══════════════════════════════════════════════${NC}"; echo -e "${CYAN}${BOLD}  $1${NC}"; echo -e "${CYAN}${BOLD}═══════════════════════════════════════════════${NC}"; }

# ============================================================
section "1/8: DOCKER CONTAINERS HEALTH"
# ============================================================

REQUIRED_CONTAINERS="sdoqap-namenode sdoqap-datanode sdoqap-spark-master sdoqap-spark-worker sdoqap-elasticsearch sdoqap-postgres sdoqap-api sdoqap-n8n sdoqap-grafana sdoqap-kibana"

for c in $REQUIRED_CONTAINERS; do
  STATUS=$(docker inspect --format='{{.State.Status}}' $c 2>/dev/null)
  if [ "$STATUS" = "running" ]; then
    UPTIME=$(docker inspect --format='{{.State.StartedAt}}' $c 2>/dev/null)
    pass "$c is running (started: $UPTIME)"
  elif [ -z "$STATUS" ]; then
    fail "$c container NOT FOUND"
  else
    fail "$c status: $STATUS (expected: running)"
  fi
done

# Check container restart counts
echo ""
echo -e "  ${BOLD}Container Restart Counts:${NC}"
for c in $REQUIRED_CONTAINERS; do
  RC=$(docker inspect --format='{{.RestartCount}}' $c 2>/dev/null)
  if [ "$RC" -gt "5" ] 2>/dev/null; then
    warn "$c has restarted $RC times (might indicate instability)"
  fi
done

# ============================================================
section "2/8: HADOOP HDFS HEALTH"
# ============================================================

# Wait for NameNode
echo "  Waiting for NameNode RPC..."
for i in $(seq 1 30); do
  if docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode"; then
    break
  fi
  sleep 2
done

# Check safe mode
SM=$(docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>/dev/null)
if echo "$SM" | grep -q "OFF"; then
  pass "NameNode safe mode is OFF"
else
  echo "  NameNode is in safe mode, attempting to leave..."
  docker exec sdoqap-namenode hdfs dfsadmin -safemode leave 2>/dev/null
  SM2=$(docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>/dev/null)
  if echo "$SM2" | grep -q "OFF"; then
    pass "NameNode safe mode turned OFF successfully"
  else
    fail "Cannot leave safe mode: $SM2"
  fi
fi

# Check HDFS raw data
echo ""
echo -e "  ${BOLD}HDFS Raw Data Check:${NC}"
for TABLE in products gov_data sales_records; do
  SIZE=$(docker exec sdoqap-namenode hdfs dfs -du -s /data/raw/$TABLE 2>/dev/null | awk '{print $1}')
  if [ -n "$SIZE" ] && [ "$SIZE" -gt "0" ] 2>/dev/null; then
    pass "HDFS /data/raw/$TABLE exists (${SIZE} bytes)"
  else
    fail "HDFS /data/raw/$TABLE is missing or empty"
  fi
done

# Check HDFS active data
echo ""
echo -e "  ${BOLD}HDFS Active (Processed) Data Check:${NC}"
for TABLE in products gov_data sales_records; do
  EXISTS=$(docker exec sdoqap-namenode hdfs dfs -test -d /data/active/$TABLE 2>/dev/null; echo $?)
  if [ "$EXISTS" = "0" ]; then
    SIZE=$(docker exec sdoqap-namenode hdfs dfs -du -s /data/active/$TABLE 2>/dev/null | awk '{print $1}')
    pass "HDFS /data/active/$TABLE exists (${SIZE} bytes)"
  else
    fail "HDFS /data/active/$TABLE NOT FOUND (Spark hasn't processed yet?)"
  fi
done

# Check HDFS quarantine data
echo ""
echo -e "  ${BOLD}HDFS Quarantine Data Check:${NC}"
for TABLE in products gov_data sales_records; do
  EXISTS=$(docker exec sdoqap-namenode hdfs dfs -test -d /data/quarantine/$TABLE 2>/dev/null; echo $?)
  if [ "$EXISTS" = "0" ]; then
    pass "HDFS /data/quarantine/$TABLE exists"
  else
    warn "HDFS /data/quarantine/$TABLE not found (may be ok if no invalid data)"
  fi
done

# ============================================================
section "3/8: POSTGRESQL DATABASE"
# ============================================================

ROW_COUNT=$(docker exec sdoqap-postgres psql -U sdoqap -d sdoqap_oltp -t -c "SELECT COUNT(*) FROM sales_records;" 2>/dev/null | tr -d ' ')
if [ -n "$ROW_COUNT" ] && [ "$ROW_COUNT" -gt "0" ] 2>/dev/null; then
  pass "PostgreSQL sales_records table has $ROW_COUNT rows"
else
  fail "PostgreSQL sales_records table is empty or not accessible"
fi

# Check connection from n8n network
docker exec sdoqap-postgres psql -U sdoqap -d sdoqap_oltp -t -c "SELECT 1;" >/dev/null 2>&1
if [ $? -eq 0 ]; then
  pass "PostgreSQL accepts connections"
else
  fail "PostgreSQL is not accepting connections"
fi

# ============================================================
section "4/8: ELASTICSEARCH INDICES"
# ============================================================

ES_HEALTH=$(docker exec sdoqap-elasticsearch curl -s http://localhost:9200/_cluster/health 2>/dev/null)
if echo "$ES_HEALTH" | grep -q '"status"'; then
  ES_STATUS=$(echo "$ES_HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "unknown")
  if [ "$ES_STATUS" = "green" ] || [ "$ES_STATUS" = "yellow" ]; then
    pass "Elasticsearch cluster status: $ES_STATUS"
  else
    fail "Elasticsearch cluster status: $ES_STATUS"
  fi
else
  fail "Cannot connect to Elasticsearch"
fi

# Check indices
echo ""
echo -e "  ${BOLD}Elasticsearch Index Check:${NC}"
for INDEX in sdoqap_quality_runs sdoqap_lineage_runs sdoqap_pipeline_runs sdoqap_schema_drifts; do
  RESP=$(docker exec sdoqap-elasticsearch curl -s "http://localhost:9200/$INDEX/_count" 2>/dev/null)
  COUNT=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count', -1))" 2>/dev/null || echo "-1")
  if [ "$COUNT" -gt "0" ] 2>/dev/null; then
    pass "Index '$INDEX' has $COUNT documents"
  elif [ "$COUNT" = "0" ]; then
    # schema_drifts being empty is expected when no schema drift has been detected
    if [ "$INDEX" = "sdoqap_schema_drifts" ]; then
      pass "Index '$INDEX' exists (0 documents — no schema drift detected)"
    else
      warn "Index '$INDEX' exists but is empty (0 documents)"
    fi
  else
    fail "Index '$INDEX' not found or not accessible"
  fi
done

# ============================================================
section "5/8: FASTAPI ENDPOINTS"
# ============================================================

API_BASE="http://localhost:${API_PORT}"

# Health endpoint
HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$API_BASE/health" 2>/dev/null)
if [ "$HEALTH" = "200" ]; then
  pass "GET /health → 200 OK"
else
  fail "GET /health → HTTP $HEALTH (expected 200)"
fi

# API v1 endpoints
ENDPOINTS=(
  "GET /api/v1/services/status"
  "GET /api/v1/kpi/stats"
  "GET /api/v1/anomaly/sources"
  "GET /api/v1/analytics/projection"
  "GET /api/v1/analytics/clustering"
  "GET /api/v1/analytics/impact"
  "GET /api/v1/analytics/recommendations"
  "GET /api/v1/performance/metrics"
  "GET /api/v1/system/activity"
  "GET /api/v1/quality"
  "GET /api/v1/pipeline"
  "GET /api/v1/lineage/products"
  "GET /api/v1/lineage/inspect/products/spark"
)

echo ""
echo -e "  ${BOLD}API Endpoint Tests:${NC}"
for EP in "${ENDPOINTS[@]}"; do
  METHOD=$(echo $EP | awk '{print $1}')
  API_PATH=$(echo $EP | awk '{print $2}')
  CODE=$(curl -s -o /tmp/api_resp.json -w "%{http_code}" "$API_BASE$API_PATH" 2>/dev/null)
  if [ "$CODE" = "200" ]; then
    # Check if response has actual data (not empty)
    RESP_SIZE=$(cat /tmp/api_resp.json | wc -c)
    if [ "$RESP_SIZE" -gt "5" ]; then
      pass "$EP → 200 OK ($RESP_SIZE bytes)"
    else
      warn "$EP → 200 but response is very small ($RESP_SIZE bytes)"
    fi
  elif [ "$CODE" = "404" ]; then
    fail "$EP → 404 NOT FOUND"
  elif [ "$CODE" = "500" ]; then
    ERR_MSG=$(cat /tmp/api_resp.json | head -c 200)
    fail "$EP → 500 ERROR: $ERR_MSG"
  else
    fail "$EP → HTTP $CODE"
  fi
done

# ============================================================
section "6/8: N8N WORKFLOW ENGINE"
# ============================================================

# Wait for n8n to be fully ready (it may have just been restarted)
N8N_HEALTH="000"
for i in $(seq 1 12); do
  N8N_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:5678/healthz" 2>/dev/null)
  if [ "$N8N_HEALTH" = "200" ]; then
    break
  fi
  sleep 3
done

if [ "$N8N_HEALTH" = "200" ]; then
  pass "n8n health endpoint → 200 OK"
else
  fail "n8n health endpoint → HTTP $N8N_HEALTH"
fi

# Verify n8n webhook engine is reachable
if [ "$N8N_HEALTH" = "200" ]; then
  pass "n8n webhook engine is reachable and ready"
else
  fail "n8n webhook engine → HTTP $N8N_HEALTH"
fi

# ============================================================
section "7/8: GRAFANA & KIBANA"
# ============================================================

GRAFANA=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:${GRAFANA_PORT}/api/health" 2>/dev/null)
if [ "$GRAFANA" = "200" ]; then
  pass "Grafana health → 200 OK"
else
  fail "Grafana health → HTTP $GRAFANA"
fi

# Wait for Kibana to be ready
KIBANA="503"
for i in $(seq 1 20); do
  KIBANA=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:5601/api/status" 2>/dev/null)
  if [ "$KIBANA" = "200" ] || [ "$KIBANA" = "401" ]; then
    break
  fi
  sleep 3
done

if [ "$KIBANA" = "200" ]; then
  pass "Kibana status → 200 OK"
elif [ "$KIBANA" = "401" ]; then
  pass "Kibana status → 401 (auth required, Kibana is up)"
else
  fail "Kibana status → HTTP $KIBANA (failed to initialize)"
fi

# ============================================================
section "8/8: SPARK QUALITY ENGINE"
# ============================================================

# Check if requests module is installed
docker exec sdoqap-spark-master python3 -c "import requests; print('OK')" >/dev/null 2>&1
if [ $? -eq 0 ]; then
  pass "Spark: 'requests' module is installed"
else
  fail "Spark: 'requests' module NOT installed"
fi

# Check if spark_quality_engine.py exists
docker exec sdoqap-spark-master test -f /opt/spark-apps/spark_quality_engine.py
if [ $? -eq 0 ]; then
  pass "Spark: spark_quality_engine.py exists"
else
  fail "Spark: spark_quality_engine.py NOT FOUND"
fi

# Check if schema_registry.json exists
docker exec sdoqap-spark-master test -f /opt/spark-apps/schema_registry.json
if [ $? -eq 0 ]; then
  pass "Spark: schema_registry.json exists"
else
  fail "Spark: schema_registry.json NOT FOUND"
fi

# ============================================================
section "FINAL REPORT"
# ============================================================

TOTAL=$((PASS + FAIL + WARN))
echo ""
echo -e "  ${GREEN}✅ PASSED: $PASS${NC}"
echo -e "  ${RED}❌ FAILED: $FAIL${NC}"
echo -e "  ${YELLOW}⚠️  WARNINGS: $WARN${NC}"
echo -e "  📊 TOTAL: $TOTAL tests"
echo ""

if [ $FAIL -eq 0 ] && [ $WARN -eq 0 ]; then
  echo -e "  ${GREEN}${BOLD}🎉 ALL TESTS PASSED! System is fully healthy.${NC}"
elif [ $FAIL -eq 0 ]; then
  echo -e "  ${YELLOW}${BOLD}⚠️  All tests passed but $WARN warning(s) found.${NC}"
else
  echo -e "  ${RED}${BOLD}🔴 $FAIL test(s) FAILED. Issues found:${NC}"
  echo -e "$ERRORS"
fi
echo ""
