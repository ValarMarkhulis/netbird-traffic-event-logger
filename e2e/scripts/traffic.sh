#!/usr/bin/env bash
# Generates allowed, blocked and routed traffic between the lab peers and
# prints each result next to what the bootstrap policies should allow.
# Repeat with ROUNDS=n to get more events per flow.
# Afterwards, long-lived flows run in parallel for LONG_SECONDS (default 45,
# 0 skips them) so start and end events are spread out in time.
cd "$(dirname "$0")/.."
source scripts/lib.sh

LAN_APP=172.30.0.10
TOKEN=$(get_token)
peers=$(api GET /peers) || die "Listing peers failed"
ip_of() {
  jq -er --arg h "$1" '[.[] | select(.hostname == $h)][0].ip' <<<"$peers" \
    || die "$1 not found in management — run 'make peers' first"
}
WEB=$(ip_of web-server)
DB=$(ip_of db-server)
KERNEL=$(ip_of kernel-server)
# check-events.sh only looks at events from this run onwards.
date -u +%FT%TZ > .last-traffic-run

mismatches=0
# probe <from> <expect allow|block> <label> <command run inside <from>>
probe() {
  local from="$1" expect="$2" label="$3" cmd="$4" got
  if docker compose exec -T "$from" sh -c "$cmd" >/dev/null 2>&1; then got=allow; else got=block; fi
  local mark="ok"
  [ "$got" = "$expect" ] || { mark="UNEXPECTED"; mismatches=$((mismatches + 1)); }
  printf '  %-13s %-34s %-6s (expected %-5s) %s\n' "$from" "$label" "$got" "$expect" "$mark"
}
http() { echo "curl -sf --max-time 3 http://$1/"; }
tcp()  { echo "echo | socat -T2 - TCP:$1,connect-timeout=3 | grep -q tcp"; }
udp()  { echo "echo ping | socat -T2 - UDP:$1 | grep -q udp"; }
icmp() { echo "ping -c 2 -W 2 $1"; }
sctp() { echo "echo | socat -T2 - SCTP-CONNECT:$1,connect-timeout=3 | grep -q sctp"; }
# raw <ip> <protocol> <payload>: one raw IP packet; there is no reply, so it's
# never judged allowed/blocked here — check-events.sh checks what gets reported.
GRE='\000\000\010\000hello-gre'   # GRE v0 header (no flags, IPv4 payload type)
raw()  { echo "printf '$3' | socat -u - IP4-SENDTO:$1:$2"; }
send() {
  local from="$1" label="$2" cmd="$3" got=sent
  docker compose exec -T "$from" sh -c "$cmd" >/dev/null 2>&1 || got=FAILED
  [ "$got" = sent ] || mismatches=$((mismatches + 1))
  printf '  %-13s %-34s %s\n' "$from" "$label" "$got"
}

for round in $(seq "${ROUNDS:-1}"); do
  log "Round $round — web-server $WEB, db-server $DB, lan-app $LAN_APP (via lan-router)"

  probe dev-laptop allow "HTTP  -> web-server:8000"   "$(http "$WEB:8000")"
  probe dev-laptop block "HTTP  -> web-server:8443"   "$(http "$WEB:8443")"
  probe dev-laptop block "ICMP  -> web-server"        "$(icmp "$WEB")"
  probe dev-laptop block "TCP   -> db-server:5432"    "$(tcp "$DB:5432")"
  probe dev-laptop allow "HTTP  -> lan-app:80 (routed)" "$(http "$LAN_APP:80")"
  probe dev-laptop block "ICMP  -> lan-app (routed)"  "$(icmp "$LAN_APP")"

  probe ops-laptop allow "ICMP  -> web-server"        "$(icmp "$WEB")"
  probe ops-laptop allow "HTTP  -> web-server:8443"   "$(http "$WEB:8443")"
  probe ops-laptop allow "TCP   -> db-server:5432"    "$(tcp "$DB:5432")"
  probe ops-laptop allow "UDP   -> db-server:514"     "$(udp "$DB:514")"
  probe ops-laptop allow "ICMP  -> lan-app (routed)"  "$(icmp "$LAN_APP")"
  probe ops-laptop allow "HTTP  -> lan-app:80 (routed)" "$(http "$LAN_APP:80")"

  probe web-server allow "TCP   -> db-server:5432"    "$(tcp "$DB:5432")"
  probe web-server block "UDP   -> db-server:514"     "$(udp "$DB:514")"

  probe db-server  block "HTTP  -> web-server:8000"   "$(http "$WEB:8000")"

  # Protocols beyond TCP/UDP/ICMP (see the README's protocol table).
  # Userspace peers silently drop every inbound packet that isn't TCP/UDP/ICMP,
  # whatever the policy says. So SCTP fails even where allowed: here
  # ops-laptop drops kernel-server's replies, and db-server drops the request.
  probe ops-laptop block "SCTP  -> kernel-server:3868"  "$(sctp "$KERNEL:3868")"
  send  ops-laptop       "GRE   -> kernel-server"       "$(raw "$KERNEL" 47 "$GRE")"
  probe dev-laptop block "SCTP  -> kernel-server:3868"  "$(sctp "$KERNEL:3868")"
  probe ops-laptop block "SCTP  -> db-server:3868"      "$(sctp "$DB:3868")"
  send  ops-laptop       "GRE   -> db-server"           "$(raw "$DB" 47 "$GRE")"
  probe web-server block "SCTP  -> db-server:3868"      "$(sctp "$DB:3868")"
  send  web-server       "GRE   -> db-server"           "$(raw "$DB" 47 "$GRE")"

done

# ---------------------------------------------------------- long-lived flows
LONG=${LONG_SECONDS:-45}
if [ "$LONG" -gt 0 ]; then
  rate=$(( 2097152 / LONG ))   # bytes/s so the 2 MiB big.bin takes ~LONG seconds
  log "Long-lived flows for ~${LONG}s, in parallel"
  # long <from> <label> <command>
  long() {
    local from="$1" label="$2" cmd="$3" start=$SECONDS
    printf '  %-11s %-34s started\n' "$from" "$label"
    if docker compose exec -T "$from" sh -c "$cmd" >/dev/null 2>&1; then
      printf '  %-11s %-34s done after %ss\n' "$from" "$label" $((SECONDS - start))
    else
      printf '  %-11s %-34s FAILED after %ss\n' "$from" "$label" $((SECONDS - start))
      return 1
    fi
  }
  # One connection/flow each, kept alive by sending something every second.
  every_second() { echo "i=0; while [ \$i -lt $LONG ]; do echo tick \$i; i=\$((i+1)); sleep 1; done"; }
  pids=()
  long dev-laptop "HTTP  slow download web:8000"     "curl -sf --limit-rate $rate -o /dev/null http://$WEB:8000/big.bin" & pids+=($!)
  long ops-laptop "TCP   session db-server:5432"     "($(every_second)) | socat -T5 - TCP:$DB:5432" & pids+=($!)
  long ops-laptop "UDP   stream db-server:514"       "($(every_second)) | socat -T5 - UDP:$DB:514,sourceport=40514" & pids+=($!)
  long ops-laptop "ICMP  ping lan-app (routed)"      "ping -c $LONG -i 1 $LAN_APP" & pids+=($!)
  long dev-laptop "HTTP  slow download lan-app (routed)" "curl -sf --limit-rate $rate -o /dev/null http://$LAN_APP/big.bin" & pids+=($!)
  for pid in "${pids[@]}"; do wait "$pid" || mismatches=$((mismatches + 1)); done
fi

[ "$mismatches" -eq 0 ] || log "$mismatches result(s) differ from the policies — check 'make logs s=<peer>'"
log "Clients report every ${FLOW_INTERVAL_MINUTES:-1} min, the ETL runs every ${ETL_INTERVAL:-60}s."
log "Then see Traffic Events in the dashboard, 'make check-events', or 'make flow-events' / 'make sessions'."
