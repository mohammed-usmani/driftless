#!/usr/bin/env bash
# Load test: three scenarios against the compose stack, written to docs/loadtest/results-<date>.md.
#   ./scripts/loadtest.sh [all|throughput|ollama|migration]
#
# 1. throughput  a throwaway stack (project driftless-loadtest) with the fake embedder: raise the write
#                rate until fewer than 99% of changes reach the index within 2 s
# 2. ollama      the demo stack, embedded by Ollama: freshness and embedding work saved under the default mix
# 3. migration   the demo stack: switch to mxbai-embed-large while writes and searches keep running
#
# Scenario 1 stops the demo stack while it runs (its volumes are kept). Scenarios 2 and 3 expect it
# seeded (./scripts/seed.sh). Scenario 3 leaves the demo on the new model.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
WHAT=${1:-all}
API=http://localhost:8180
AUTH="Authorization: Bearer $DRIFTLESS_ADMIN_TOKEN"
PROM=http://localhost:9091
LOADGEN=driftless-loadgen/target/driftless-loadgen.jar
TARGET_MODEL=${TARGET_MODEL:-mxbai-embed-large}
TARGET_DIMS=${TARGET_DIMS:-1024}
RATES=${RATES:-100 200 400 600 800 1000 1200 1600}
# inserts balance deletes, so the table keeps its size across steps
THROUGHPUT_MIX=${THROUGHPUT_MIX:-insert=10,title=20,body=20,touch=30,acl=10,delete=10}
STEP=${STEP:-60s}
mkdir -p docs/loadtest
OUT=docs/loadtest/results-$(date +%F).md
WORK=.superpowers/loadtest; mkdir -p "$WORK"

log() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$WORK/run.log" >&2; }
api() { curl -fsS -H "$AUTH" -H 'Content-Type: application/json' "$@"; }
prom() { curl -fsS "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "0"'; }
# "<within 2 s> <count> <instances reporting>": summed per instance, so an instance missing a scrape shows up instead of skewing the sum
freshness() {
  curl -fsS "$PROM/api/v1/query" --data-urlencode 'query=driftless_freshness_seconds_bucket{le="2.0"} or driftless_freshness_seconds_count' \
    | jq -r '[.data.result[] | select(.metric.le) | .value[1] | tonumber] as $le | [.data.result[] | select(.metric.le | not) | .value[1] | tonumber] as $n | "\($le | add // 0) \($n | add // 0) \($n | length)"'
}
# loadgen_threads <rate> <duration>: one connection per 100 changes/s, each thread on its own slice of rows
loadgen_threads() {
  loadgen run --rate "$1" --duration "$2" --threads $(( ($1 + 99) / 100 )) --mix "$THROUGHPUT_MIX" 2>&1 | tail -1
}
index_sql() { docker compose exec -T index-db psql -U driftless -d driftless -v ON_ERROR_STOP=1 -tAF' ' -c "$1"; }
loadgen() {
  SOURCE_URL="jdbc:postgresql://localhost:${SOURCE_DB_PORT:-5443}/source" SOURCE_USER=app SOURCE_PASSWORD="$SOURCE_APP_PASSWORD" \
    exec java -jar "$LOADGEN" "$@" # exec: a backgrounded call's $! is then the JVM itself
}
# records behind on the change topics, read from Kafka itself (the lag gauge is only refreshed every 15 s)
kafka_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group driftless 2>/dev/null | awk '$1 == "driftless" && $6 ~ /^[0-9]+$/ { s += $6 } END { print s + 0 }'
}
drain() { # drain <seconds>: wait for the consumer to catch up; non-zero if it doesn't in time
  for _ in $(seq "$1"); do [[ $(kafka_lag) == 0 ]] && return 0; sleep 1; done; return 1
}
healthy() { docker compose up -d --wait "$@" > "$WORK/compose.log" 2>&1; }
percentile() { sort -n | awk -v p="$1" '{ v[NR] = $1 } END { i = int(NR * p + 0.999999); if (i < 1) i = 1; print v[i] }'; }
section() { printf '\n## %s\n\n' "$*" >> "$OUT"; }
raw() { { echo '```'; cat; echo '```'; } >> "$OUT"; }

[[ -f $LOADGEN ]] || ./mvnw -B -q -pl driftless-loadgen -am package -DskipTests
if [[ ! -f $OUT ]]; then
  cat > "$OUT" <<EOF
# Load test results, $(date +%F)

Produced by \`scripts/loadtest.sh\` at commit \`$(git rev-parse --short HEAD)\`. Every number below comes from the raw output under it.

- Host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cores, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GB RAM, $(uname -s)
- Everything (both Postgres instances, Kafka, Connect, two Driftless instances, Prometheus) runs in Docker on that one machine; Ollama runs natively on the host.
- Load comes from \`driftless-loadgen\`: one statement per change, autocommit, one JDBC connection per thread; scenario 1 runs one thread per 100 changes/s, each on its own slice of rows, with inserts balancing deletes.
- Freshness is source commit time to the moment the index transaction commits.
EOF
fi

throughput() {
  log "scenario 1: stopping the demo stack, starting a throwaway one with the fake embedder"
  docker compose stop > /dev/null 2>&1
  (
    export COMPOSE_PROJECT_NAME=driftless-loadtest ARTICLES_EMBED_PROVIDER=fake ARTICLES_EMBED_MODEL=fake ARTICLES_EMBED_DIMS=768
    healthy --build
    ./scripts/seed.sh 5000 > "$WORK/seed-fake.log"
    section "1. Pipeline throughput (fake embedder, 768 dims, 0 ms)"
    echo "Each step writes $THROUGHPUT_MIX for $STEP, then waits for the consumer group to drain. A step passes when at least 99% of its changes reached the index within 2 s (the 2 s histogram bucket over the count, summed over both instances)." >> "$OUT"
    echo >> "$OUT"
    echo "| target changes/s | achieved changes/s | changes applied | within 2 s | drained | p99 under 2 s |" >> "$OUT"
    echo "|---|---|---|---|---|---|" >> "$OUT"
    best=none
    step=0
    : > "$WORK/throughput.raw"
    for rate in $RATES; do
      step=$(( step + 1 ))
      sleep 16 # a fresh scrape, so the "before" values are current
      read -r le0 n0 inst0 <<< "$(freshness)"
      gen=$(loadgen_threads "$rate" "$STEP")
      sleep 3; drained=yes; drain 180 || drained=no
      sleep 16
      read -r le1 n1 inst1 <<< "$(freshness)"
      if [[ $inst0 != 2 || $inst1 != 2 ]]; then
        echo "rate=$rate: an instance was missing from Prometheus ($inst0 then $inst1 reporting), step not valid" | tee -a "$WORK/throughput.raw" >&2
        break
      fi
      n=$(echo "$n1 - $n0" | bc); within=$(echo "$le1 - $le0" | bc)
      achieved=$(sed -E 's/^([0-9]+) ops in ([0-9.]+)s.*\(([0-9]+) hit no row\)/scale=1; (\1 - \3) \/ \2/' <<< "$gen" | bc) # rows actually changed per second
      share=$(echo "scale=4; if ($n > 0) $within / $n else 0" | bc)
      ok=$(echo "$share >= 0.99" | bc); [[ $drained == yes ]] || ok=0
      printf 'rate=%s drained=%s changes=%s within2s=%s share=%s loadgen=[%s]\n' "$rate" "$drained" "$n" "$within" "$share" "$(paste -sd';' - <<< "$gen")" | tee -a "$WORK/throughput.raw" >&2
      printf '| %s | %s | %.0f | %.2f%% | %s | %s |\n' "$rate" "$achieved" "$n" "$(echo "$share * 100" | bc)" "$drained" "$([[ $ok == 1 ]] && echo yes || echo no)" >> "$OUT"
      [[ $ok == 1 ]] || break
      best=$achieved
      # the generator is the ceiling when it can't reach the target rate
      if [[ $(echo "$achieved < $rate * 0.9" | bc) == 1 ]]; then echo "generator-bound" >> "$WORK/throughput.raw"; break; fi
    done
    echo >> "$OUT"
    if grep -q generator-bound "$WORK/throughput.raw"; then
      echo "**Highest sustained rate with p99 freshness under 2 s: at least $best changes/s.** The load generator topped out there, so this is a floor for Driftless, not its limit." >> "$OUT"
    else
      echo "**Highest sustained rate with p99 freshness under 2 s: $best changes/s.**" >> "$OUT"
    fi
    echo >> "$OUT"; echo "Raw output:" >> "$OUT"; raw < "$WORK/throughput.raw"
    log "scenario 1: removing the throwaway stack"
    docker compose down -v > /dev/null 2>&1
  )
  healthy
}

ollama_mix() {
  local rate=${OLLAMA_RATE:-10} duration=${OLLAMA_DURATION:-90s}
  log "scenario 2: default mix at $rate ops/s for $duration through Ollama"
  healthy
  drain 300 || { log "consumer not caught up before scenario 2"; exit 1; }
  t0=$(index_sql "SELECT now()")
  gen=$(loadgen run --rate "$rate" --duration "$duration" 2>&1 | tail -1)
  drained=yes; drain 600 || drained=no
  # recent_change keeps the newest 1000 changes per pipeline, enough for this window
  stats=$(index_sql "SELECT count(*),
      round(percentile_cont(0.5) WITHIN GROUP (ORDER BY freshness_ms)),
      round(percentile_cont(0.99) WITHIN GROUP (ORDER BY freshness_ms)),
      max(freshness_ms),
      count(*) FILTER (WHERE outcome IN ('SKIP', 'METADATA_ONLY')),
      coalesce(sum(cache_hits), 0), coalesce(sum(chunks_embedded), 0)
    FROM recent_change WHERE pipeline = 'articles' AND NOT snapshot AND applied_at >= '$t0'")
  outcomes=$(index_sql "SELECT outcome, count(*) FROM recent_change WHERE pipeline = 'articles' AND NOT snapshot AND applied_at >= '$t0' GROUP BY 1 ORDER BY 2 DESC")
  read -r n p50 p99 max noembed hits embedded <<< "$stats"
  section "2. Realistic mix through Ollama ($ARTICLES_EMBED_MODEL)"
  cat >> "$OUT" <<EOF
The default mix (insert 5, title 20, body 20, touch 35, acl 10, delete 10) at $rate changes/s for $duration, embedded by \`$ARTICLES_EMBED_MODEL\` on the host. Freshness and outcomes come from \`recent_change\` for exactly this window.

| changes | p50 freshness | p99 freshness | max | needed no embedding | chunk cache hits | chunks sent to Ollama |
|---|---|---|---|---|---|---|
| $n | ${p50} ms | ${p99} ms | ${max} ms | $noembed ($(echo "scale=1; 100 * $noembed / $n" | bc)%) | $hits | $embedded |

Embedding work saved: $(echo "scale=1; 100 * ($noembed + $hits) / ($n + $hits)" | bc)% (changes that needed no embedding, plus chunks served from the cache, over all changes plus cache hits). It depends on the mix: touches only change \`updated_at\` (SKIP), ACL edits only change metadata (METADATA_ONLY), and a body edit re-embeds only the chunks whose text changed.

Raw output:
EOF
  printf 'loadgen: %s\ndrained: %s\nn p50 p99 max no_embed cache_hits chunks_embedded: %s\noutcomes:\n%s\n' "$gen" "$drained" "$stats" "$outcomes" | raw
}

migration() {
  log "scenario 3: migrating to $TARGET_MODEL under writes and searches"
  curl -fsS "${OLLAMA_HOST_URL:-http://localhost:11434}/api/tags" | jq -e --arg m "$TARGET_MODEL" '.models[] | select(.name == $m or .name == ($m + ":latest"))' > /dev/null \
    || { echo "pull the target model first: ollama pull $TARGET_MODEL"; exit 1; }
  local from=$ARTICLES_EMBED_MODEL
  export ARTICLES_EMBED_MODEL=$TARGET_MODEL ARTICLES_EMBED_DIMS=$TARGET_DIMS
  for instance in driftless-1 driftless-2; do healthy --no-deps "$instance"; done # rolling restart onto the new config
  drain 300 || true
  : > "$WORK/search.raw"
  ( while :; do
      curl -s -o /dev/null -w '%{http_code} %{time_total}\n' -H "$AUTH" -H 'Content-Type: application/json' \
        -X POST "$API/v1/pipelines/articles/search" -d '{"query": "How do I change my password?", "k": 5}' >> "$WORK/search.raw"
      sleep 0.2
    done ) & probe=$!
  loadgen run --rate 5 --duration 24h > "$WORK/migration-loadgen.log" 2>&1 & writer=$!
  trap 'kill $probe $writer 2> /dev/null || true' EXIT
  start=$(date +%s)
  created=$(api -X POST "$API/v1/pipelines/articles/migrations" | jq -c '{number, status, model: .embedding.model}')
  while [[ $(api "$API/v1/pipelines/articles" | jq -r .migration.status) != READY ]]; do sleep 2; done
  ready=$(( $(date +%s) - start ))
  progress=$(api "$API/v1/pipelines/articles" | jq -c .migration)
  lag_at_ready=$(kafka_lag)
  api -X POST "$API/v1/pipelines/articles/migrations/cutover" > /dev/null
  sleep 10 # keep searching across the swap
  api -X POST "$API/v1/pipelines/articles/migrations/finalize" > /dev/null
  total=$(( $(date +%s) - start ))
  kill $probe $writer 2> /dev/null || true; wait 2> /dev/null || true
  # live changes written during the backfill queue behind its reads on the same partitions; time how long they take to clear
  stopped=$(date +%s); lag_at_finalize=$(kafka_lag)
  drain 3600 || true
  drained_after=$(( $(date +%s) - stopped ))
  # convergence: every source key indexed, nothing else, then the reconciler over the whole table
  live=$(api "$API/v1/pipelines/articles" | jq -r .liveGeneration)
  docker compose exec -T source-db psql -U app -d source -tAc "SELECT id FROM articles" | tr -d '\r ' | grep . | sort > "$WORK/src.keys"
  index_sql "SELECT row_key FROM row_state WHERE generation_id = $live AND NOT deleted" | tr -d '\r ' | grep . | sort > "$WORK/idx.keys"
  leaked=$(comm -13 "$WORK/src.keys" "$WORK/idx.keys" | wc -l | tr -d ' '); missing=$(comm -23 "$WORK/src.keys" "$WORK/idx.keys" | wc -l | tr -d ' ')
  before=$(api "$API/v1/pipelines/articles/drift-reports?limit=1" | jq -r '.data[0].id // 0')
  api -X POST "$API/v1/pipelines/articles/reconcile" > /dev/null
  while [[ $(api "$API/v1/pipelines/articles/drift-reports?limit=1" | jq -r '.data[0].id // 0') == "$before" ]]; do sleep 2; done
  drift=$(api "$API/v1/drift-reports/$(api "$API/v1/pipelines/articles/drift-reports?limit=1" | jq -r '.data[0].id')" | jq -c '{missing, mismatched, leaked}')
  searches=$(wc -l < "$WORK/search.raw" | tr -d ' ')
  errors=$(awk '$1 != 200' "$WORK/search.raw" | wc -l | tr -d ' ')
  p50=$(awk '$1 == 200 { print $2 * 1000 }' "$WORK/search.raw" | percentile 0.5)
  p99=$(awk '$1 == 200 { print $2 * 1000 }' "$WORK/search.raw" | percentile 0.99)
  section "3. Migration under writes: $from to $TARGET_MODEL"
  cat >> "$OUT" <<EOF
A full re-embed of the seeded articles into a new generation while the load generator writes 5 changes/s and a client searches every 200 ms. Timed from \`POST /migrations\` to finalize (the old generation dropped). Then the writer stops, the queued live changes drain, and the index is compared with the source.

| source rows at READY | building to READY | start to finalize | searches | search errors | search p50 | search p99 |
|---|---|---|---|---|---|---|
| $(jq -r .sourceRows <<< "$progress") | ${ready} s | ${total} s | $searches | $errors | $(printf '%.0f' "$p50") ms | $(printf '%.0f' "$p99") ms |

| changes queued at READY | at finalize | drained after the writer stopped | keys leaked / missing | reconciler (whole table) |
|---|---|---|---|---|
| $lag_at_ready | $lag_at_finalize | ${drained_after} s | $leaked / $missing | $drift |

Search latency includes embedding the query with the live model on the same Ollama that is busy embedding the backfill. Live changes share partitions with the backfill's reads, so while a slow model works through the backfill they wait behind it, in both generations; the queue sizes above show how far behind they fell.

Raw output:
EOF
  printf 'created: %s\nready after %ss: %s\nfinalized after %ss\nlag at ready: %s, at finalize: %s, drained %ss after the writer stopped\nkeys: source %s, index %s, leaked %s, missing %s\nreconciler: %s\nsearch status counts:\n%s\n' \
    "$created" "$ready" "$progress" "$total" "$lag_at_ready" "$lag_at_finalize" "$drained_after" \
    "$(wc -l < "$WORK/src.keys" | tr -d ' ')" "$(wc -l < "$WORK/idx.keys" | tr -d ' ')" "$leaked" "$missing" "$drift" \
    "$(awk '{ print $1 }' "$WORK/search.raw" | sort | uniq -c)" | raw
  log "scenario 3 done: set ARTICLES_EMBED_MODEL=$TARGET_MODEL and ARTICLES_EMBED_DIMS=$TARGET_DIMS in .env to keep the demo on it"
}

case $WHAT in
  throughput) throughput ;;
  ollama) ollama_mix ;;
  migration) migration ;;
  all) throughput; ollama_mix; migration ;;
  *) echo "usage: $0 [all|throughput|ollama|migration]"; exit 2 ;;
esac
log "results: $OUT"
