#!/usr/bin/env bash
# A scripted walk-through: search, edit, delete, a zero-downtime model migration, and drift repair.
# Run ./scripts/seed.sh first. Migrates to mxbai-embed-large by default; override with
#   TARGET_MODEL=... TARGET_DIMS=... TARGET_MAX_CHARS=... ./scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
API=http://localhost:8180
AUTH="Authorization: Bearer $DRIFTLESS_ADMIN_TOKEN"
TARGET_MODEL=${TARGET_MODEL:-mxbai-embed-large}
TARGET_DIMS=${TARGET_DIMS:-1024}
TARGET_MAX_CHARS=${TARGET_MAX_CHARS:-${ARTICLES_MAX_CHARS:-2000}}
QUERY="How do I change my password?"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
api() { curl -fsS -H "$AUTH" -H 'Content-Type: application/json' "$@"; }
search() {
  api -X POST "$API/v1/pipelines/articles/search" -d "$(jq -n --arg q "$1" '{query: $q, k: 3}')"
}
show() { jq -r '"   generation \(.generation)", (.results[] | "   \(.score*1000|round/1000)  #\(.key)  \(.text|split("\n")[0])")'; }
sql() { docker compose exec -T source-db psql -U app -d source -v ON_ERROR_STOP=1 -tAc "$1"; }
wait_for() { # wait_for <seconds> <description> <command...>
  local limit=$1 what=$2; shift 2
  for _ in $(seq "$limit"); do "$@" > /dev/null 2>&1 && return 0; sleep 1; done
  echo "   timed out waiting for: $what"; exit 1
}

missing=()
for m in "$ARTICLES_EMBED_MODEL" "$TARGET_MODEL"; do
  curl -fsS "${OLLAMA_HOST_URL:-http://localhost:11434}/api/tags" | jq -e --arg m "$m" '.models[] | select(.name == $m or .name == ($m + ":latest"))' > /dev/null \
    || missing+=("$m")
done
if (( ${#missing[@]} )); then
  echo "Ollama is missing these embedding models. Pull them, then run this again:"
  for m in "${missing[@]}"; do echo "   ollama pull $m"; done
  exit 1
fi

step "1. The index: articles synced from Postgres"
api "$API/v1/pipelines/articles" | jq '{status, liveGeneration, rows, chunks, embedCallsSaved, failedOpen}'

step "2. Search: \"$QUERY\""
search "$QUERY" | show
KEY=$(search "$QUERY" | jq -r '.results[0].key')

step "3. Edit article #$KEY in the source database (a plain UPDATE, nothing else)"
sql "UPDATE articles SET title = '[Edited] ' || title, updated_at = now() WHERE id = $KEY" > /dev/null
start=$(date +%s)
wait_for 60 "the edit to reach the index" bash -c "$(declare -f api search); API=$API AUTH='$AUTH'; search '$QUERY' | jq -e '.results[] | select(.key == \"$KEY\") | select(.text | startswith(\"[Edited]\"))'"
echo "   searchable after ~$(( $(date +%s) - start ))s"

step "4. Search again: the edited title is what comes back"
search "$QUERY" | show

step "5. Delete article #$KEY in the source"
sql "DELETE FROM articles WHERE id = $KEY" > /dev/null
wait_for 60 "the delete to reach the index" bash -c "$(declare -f api search); API=$API AUTH='$AUTH'; ! search '$QUERY' | jq -e '.results[] | select(.key == \"$KEY\")'"
search "$QUERY" | show
echo "   #$KEY is gone"

step "6. Migrate to $TARGET_MODEL ($TARGET_DIMS dims) while search keeps serving"
export ARTICLES_EMBED_MODEL=$TARGET_MODEL ARTICLES_EMBED_DIMS=$TARGET_DIMS ARTICLES_MAX_CHARS=$TARGET_MAX_CHARS
for instance in driftless-1 driftless-2; do  # rolling restart onto the new config
  docker compose up -d --no-deps "$instance" > /dev/null 2>&1
  wait_for 180 "$instance to be healthy" bash -c "[ \"\$(docker inspect -f '{{.State.Health.Status}}' \$(docker compose ps -q $instance))\" = healthy ]"
done
api -X POST "$API/v1/pipelines/articles/migrations" | jq '{number, status, model: .embedding.model, dimensions: .embedding.dimensions}'
while true; do
  m=$(api "$API/v1/pipelines/articles" | jq -c .migration)
  printf '\r   %s' "$(jq -r '"\(.status)  \(.rows) / \(.sourceRows) rows"' <<< "$m")   "
  [[ $(jq -r .status <<< "$m") == READY ]] && break
  sleep 3
done
echo
search "$QUERY" | show
echo "   (still served by the old generation)"

step "7. Cut over: one transaction swaps the live generation"
api -X POST "$API/v1/pipelines/articles/migrations/cutover" | jq '{number, status, model: .embedding.model}'
search "$QUERY" | show
api -X POST "$API/v1/pipelines/articles/migrations/finalize" > /dev/null
echo "   old generation dropped. To keep this model after a restart, set ARTICLES_EMBED_MODEL=$TARGET_MODEL and ARTICLES_EMBED_DIMS=$TARGET_DIMS in .env"

step "8. Break the index on purpose, then let the reconciler find and fix it"
api -X POST "$API/v1/chaos/pipelines/articles/corrupt" -d '{"count": 5}' | jq -c '{corrupted: .keys}'
api -X POST "$API/v1/chaos/pipelines/articles/leak" -d '{"count": 3}' | jq -c '{leaked: .keys}'
latest() { api "$API/v1/pipelines/articles/drift-reports?limit=1" | jq -r '.data[0].id // 0'; }
reconcile() {
  local before; before=$(latest)
  api -X POST "$API/v1/pipelines/articles/reconcile" > /dev/null
  while [[ $(latest) == "$before" ]]; do sleep 2; done
  api "$API/v1/drift-reports/$(latest)" | jq -c '{report: .id, missing, mismatched, leaked}'
}
reconcile
REPORT=$(latest)
api -X POST "$API/v1/drift-reports/$REPORT/repair" > /dev/null
echo "   repairing: re-read the stale rows through Debezium, synthetic deletes for the leaks"
sleep 20
reconcile

step "Done. Grafana: http://localhost:3002   API docs: $API/v3/api-docs"
