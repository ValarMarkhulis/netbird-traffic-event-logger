# Shared helpers for the local e2e scripts. Source, don't execute.
set -euo pipefail

MGMT_URL="${MGMT_URL:-http://localhost:8081}"
DEX_URL="${DEX_URL:-http://localhost:5556/dex}"
NB_USER="${NB_USER:-admin@netbird.local}"
NB_PASSWORD="${NB_PASSWORD:-password}"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

wait_for() {
  local url="$1" name="$2" tries=60
  log "Waiting for $name ($url)"
  until curl -s -o /dev/null "$url"; do
    tries=$((tries - 1))
    [ "$tries" -gt 0 ] || die "$name did not come up"
    sleep 2
  done
}

# Gets an ID token from Dex using the password grant (same client the dashboard uses).
get_token() {
  local resp
  resp=$(curl -sf -X POST "$DEX_URL/token" \
    -d grant_type=password \
    -d client_id=netbird-dashboard \
    -d "username=$NB_USER" \
    -d "password=$NB_PASSWORD" \
    -d "scope=openid email profile") || die "Dex token request failed"
  jq -er .id_token <<<"$resp" || die "No id_token in Dex response: $resp"
}

api() {
  local method="$1" path="$2"; shift 2
  curl -sf -X "$method" "$MGMT_URL/api$path" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" "$@"
}

PSQL=(docker compose exec -T flow-postgres psql -q -v ON_ERROR_STOP=1 -U netbird -d netbird_traffic_event_logging)

# lab_mapping_sql prints SQL that creates temp tables mapping this lab's
# generated identifiers to stable names, for scripts/seed*.sh:
#   m_peer(hostname, ip, key)  peer hostname <-> NetBird IP and WireGuard key
#   m_rule(id, name)           policy rule ID <-> "<policy>/<rule>"
#   m_res(id, name)            Network resource ID <-> resource name
# Public keys aren't in the API, so they come from management's store.
lab_mapping_sql() {
  local peers keys rules resources net
  peers=$(api GET /peers | jq '[.[] | {hostname, ip}]') || die "Listing peers failed"
  keys=$(docker compose exec -T management sqlite3 -json /var/lib/netbird/store.db \
    "SELECT meta_hostname AS hostname, key FROM peers") || die "Reading peer keys from management's store failed"
  rules=$(api GET /policies | jq '[.[] | .name as $p | .rules[] | {id, name: ($p + "/" + .name)}]') || die "Listing policies failed"
  resources='[]'
  for net in $(api GET /networks | jq -r '.[].id'); do
    resources=$(jq -s 'add' <(echo "$resources") <(api GET "/networks/$net/resources" | jq '[.[] | {id, name}]'))
  done
  # SQL string literals: $sq is a single quote, doubled inside the value.
  jq -nr --arg sq "'" --argjson peers "$peers" --argjson keys "${keys:-[]}" \
    --argjson rules "$rules" --argjson res "$resources" '
    def lit: $sq + (tostring | gsub($sq; $sq + $sq)) + $sq;
    def insert(table; rows; cols):
      if (rows | length) == 0 then empty else
        "INSERT INTO \(table) VALUES " + ([rows[] | "(" + ([.[cols[]] | lit] | join(",")) + ")"] | join(",")) + ";"
      end;
    ($keys | map({(.hostname): .key}) | add // {}) as $k
    | "CREATE TEMP TABLE m_peer (hostname text, ip text, key text);",
      insert("m_peer"; [$peers[] | . + {key: ($k[.hostname] // "")}]; ["hostname", "ip", "key"]),
      "CREATE TEMP TABLE m_rule (id text, name text);",
      insert("m_rule"; $rules; ["id", "name"]),
      "CREATE TEMP TABLE m_res (id text, name text);",
      insert("m_res"; $res; ["id", "name"])'
}
