#!/usr/bin/env bash
# Sets up the e2e lab in management. Idempotent: everything is matched by
# name and only created when missing.
#
#   groups     developers, ops, web-servers, databases, kernel-servers, routers,
#              office-lan
#   policies   replace the allow-all "Default" policy with role-based rules
#   network    "Office LAN" (172.30.0.0/24), routed by the routers group
#   setup keys one per role, auto-assigning the group; saved to .env as
#              SETUP_KEY_<GROUP> for docker-compose.
cd "$(dirname "$0")/.."
source scripts/lib.sh

wait_for "$DEX_URL/.well-known/openid-configuration" dex
wait_for "$MGMT_URL/api/users" management

TOKEN=$(get_token)

log "Logging in to management (creates the account on first run)"
user=$(api GET /users/current) || die "Management rejected the token — check 'make logs s=management'"
# No IdP manager is configured, so management doesn't know the user's email.
jq -r '"    user id: \(.id)  role: \(.role)"' <<<"$user"

account=$(api GET /accounts | jq '.[0]')
jq -r '"    account: \(.id)  traffic events enabled: \(.settings.extra.network_traffic_logs_enabled // false)"' <<<"$account"

# ------------------------------------------------------------------ groups
declare -A GID
for g in developers ops web-servers databases kernel-servers routers office-lan; do
  id=$(api GET /groups | jq -r --arg n "$g" '.[] | select(.name == $n) | .id' | head -1)
  if [ -z "$id" ]; then
    id=$(api POST /groups -d "$(jq -nc --arg n "$g" '{name: $n}')" | jq -er .id) || die "Creating group $g failed"
    log "Created group $g"
  fi
  GID[$g]=$id
done

# ---------------------------------------------------------------- policies
# ensure_policy <name> <protocol> <ports csv or ""> <sources csv> <destinations csv>
ensure_policy() {
  local name="$1" proto="$2" ports="$3" src="$4" dst="$5"
  if api GET /policies | jq -e --arg n "$name" 'any(.[]; .name == $n)' >/dev/null; then
    return
  fi
  local srcs dsts body
  srcs=$(for g in ${src//,/ }; do echo "${GID[$g]}"; done | jq -R . | jq -sc .)
  dsts=$(for g in ${dst//,/ }; do echo "${GID[$g]}"; done | jq -R . | jq -sc .)
  body=$(jq -nc --arg n "$name" --arg p "$proto" --arg ports "$ports" \
    --argjson s "$srcs" --argjson d "$dsts" '{
      name: $n, enabled: true, description: "local-e2e",
      rules: [{
        name: $n, enabled: true, action: "accept", bidirectional: false,
        protocol: $p, sources: $s, destinations: $d
      } + (if $ports == "" then {} else {ports: ($ports | split(","))} end)]
    }')
  api POST /policies -d "$body" >/dev/null || die "Creating policy '$name' failed"
  log "Created policy '$name'"
}

ensure_policy "Developers -> Web servers (HTTP 8000)" tcp 8000 developers web-servers
ensure_policy "Ops -> Web and DB servers (all)"       all ""   ops        web-servers,databases
ensure_policy "Web servers -> Databases (5432)"       tcp 5432 web-servers databases
ensure_policy "Ops -> Kernel servers (all)"           all ""   ops        kernel-servers
ensure_policy "Developers -> Kernel servers (SSH 22)" tcp 22   developers kernel-servers
ensure_policy "Developers -> Office LAN (HTTP 80)"    tcp 80   developers office-lan
ensure_policy "Ops -> Office LAN (all)"               all ""   ops        office-lan

default_id=$(api GET /policies | jq -r '.[] | select(.name == "Default") | .id')
if [ -n "$default_id" ]; then
  api DELETE "/policies/$default_id" >/dev/null || die "Deleting the Default policy failed"
  log "Deleted the allow-all 'Default' policy"
fi

# ----------------------------------------------------------------- network
net_id=$(api GET /networks | jq -r '.[] | select(.name == "Office LAN") | .id' | head -1)
if [ -z "$net_id" ]; then
  net_id=$(api POST /networks -d '{"name":"Office LAN","description":"docker network lan, behind lan-router"}' | jq -er .id) \
    || die "Creating network failed"
  log "Created network 'Office LAN'"
fi
if [ "$(api GET "/networks/$net_id/resources" | jq length)" -eq 0 ]; then
  api POST "/networks/$net_id/resources" -d "$(jq -nc --arg g "${GID[office-lan]}" \
    '{name: "Office LAN subnet", address: "172.30.0.0/24", enabled: true, groups: [$g]}')" >/dev/null \
    || die "Creating network resource failed"
  log "Added resource 172.30.0.0/24 to 'Office LAN'"
fi
if [ "$(api GET "/networks/$net_id/routers" | jq length)" -eq 0 ]; then
  api POST "/networks/$net_id/routers" -d "$(jq -nc --arg g "${GID[routers]}" \
    '{peer_groups: [$g], masquerade: true, metric: 9999, enabled: true}')" >/dev/null \
    || die "Creating network router failed"
  log "Added routers group as router for 'Office LAN'"
fi

# -------------------------------------------------------------- setup keys
touch .env
sed -i '/^PEER_SETUP_KEY=/d' .env   # single key from the earlier two-peer setup
for g in developers ops web-servers databases kernel-servers routers; do
  var="SETUP_KEY_$(tr 'a-z-' 'A-Z_' <<<"$g")"
  grep -q "^$var=.\+" .env && continue
  key=$(api POST /setup-keys -d "$(jq -nc --arg n "e2e-$g" --arg g "${GID[$g]}" \
    '{name: $n, type: "reusable", expires_in: 2592000, auto_groups: [$g], usage_limit: 0, ephemeral: false}')" \
    | jq -er .key) || die "Creating setup key for $g failed"
  echo "$var=$key" >> .env
  log "Created setup key e2e-$g -> .env $var"
done
