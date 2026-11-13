#!/usr/bin/env bash
# Loads the Ask Ubuntu demo articles into the source table and waits until Driftless has indexed them.
#   ./scripts/seed.sh [count]      (default 5000)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
COUNT=${1:-5000}
API=http://localhost:8180
DATA=ops/demo-data/askubuntu.jsonl

if [[ ! -s $DATA ]]; then
  echo "== fetching the Ask Ubuntu dataset (CC BY-SA)"
  ./ops/demo-data/fetch.sh --count "$COUNT"
fi
if [[ ! -f driftless-loadgen/target/driftless-loadgen.jar ]]; then
  echo "== building the load generator"
  ./mvnw -B -q -pl driftless-loadgen -am package -DskipTests
fi

echo "== seeding $COUNT articles"
SOURCE_URL="jdbc:postgresql://localhost:${SOURCE_DB_PORT:-5443}/source" SOURCE_USER=app SOURCE_PASSWORD="$SOURCE_APP_PASSWORD" \
  java -jar driftless-loadgen/target/driftless-loadgen.jar seed --file "$DATA" --limit "$COUNT"

expected=$(docker compose exec -T source-db psql -U app -d source -tAc "SELECT count(*) FROM articles")
echo "== waiting for Driftless to index $expected rows (embedding runs on your machine through Ollama)"
while true; do
  rows=$(curl -fsS -H "Authorization: Bearer $DRIFTLESS_ADMIN_TOKEN" "$API/v1/pipelines/articles" | jq -r .rows)
  printf '\r   %s / %s' "$rows" "$expected"
  [[ $rows -ge $expected ]] && break
  sleep 3
done
echo; echo "== indexed"
