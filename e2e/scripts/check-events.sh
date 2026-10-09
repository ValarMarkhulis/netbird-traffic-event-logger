#!/usr/bin/env bash
# Checks what the protocol probes from the last `make traffic` run produced
# in GET /api/events/network-traffic (what the dashboard shows). Some
# expectations are that nothing is reported: they pin down client limits.
#
#   kernel-server (kernel conntrack) reports SCTP, GRE and other protocols
#   by number, but never drops. Userspace peers silently drop inbound packets
#   that aren't TCP/UDP/ICMP before any policy check, so they report nothing.
cd "$(dirname "$0")/.."
source scripts/lib.sh

[ -f .last-traffic-run ] || die "Run 'make traffic' first"
SINCE=$(cat .last-traffic-run)
TIMEOUT=${TIMEOUT:-180}

TOKEN=$(get_token)
peers=$(api GET /peers) || die "Listing peers failed"

fetch() {
  TOKEN=$(get_token)
  api GET "/events/network-traffic?page=1&page_size=5000&start_date=$SINCE" \
    | jq --argjson peers "$peers" '
        ($peers | map({(.id): .hostname}) | add) as $names
        | [.data[] | {
            reporter: ($names[.reporter_id] // .reporter_id),
            src: (.source.name // .source.address),
            dst: (.destination.name // .destination.address),
            protocol, types: [.events[].type]
          }]'
}

# count <events> <reporter|*> <src> <dst> <protocol> <type|*>
count() {
  jq --arg r "$2" --arg s "$3" --arg d "$4" --argjson p "$5" --arg t "$6" '
    [.[] | select(($r == "*" or .reporter == $r) and .src == $s and .dst == $d and .protocol == $p
                  and ($t == "*" or (.types | index($t))))] | length' <<<"$1"
}

EXPECT=(
  # present|absent reporter  source      destination    proto type        why
  "present kernel-server ops-laptop kernel-server 132 TYPE_START kernel conntrack tracks SCTP"
  "present kernel-server ops-laptop kernel-server 132 TYPE_END   SCTP association closed"
  "present kernel-server ops-laptop kernel-server 47  TYPE_START GRE, stored by number (\"47\")"
  "absent  *             dev-laptop kernel-server 132 *          kernel mode doesn't report drops"
  "absent  *             ops-laptop db-server     132 *          userspace drops it silently, allowed or not"
  "absent  *             ops-laptop db-server     47  *          userspace drops it silently, allowed or not"
  "absent  *             web-server db-server     47  *          blocked, but also dropped silently"
)

ready() {
  local e line
  for line in "${EXPECT[@]}"; do
    read -r want r s d p t _ <<<"$line"
    [ "$want" = present ] || continue
    [ "$(count "$1" "$r" "$s" "$d" "$p" "$t")" -gt 0 ] || return 1
  done
}

log "Waiting up to ${TIMEOUT}s for the protocol probes since $SINCE to be reported"
deadline=$((SECONDS + TIMEOUT))
until events=$(fetch) && ready "$events"; do
  [ $SECONDS -lt $deadline ] || break
  sleep 10
done

failures=0
for line in "${EXPECT[@]}"; do
  read -r want r s d p t why <<<"$line"
  n=$(count "$events" "$r" "$s" "$d" "$p" "$t")
  if { [ "$want" = present ] && [ "$n" -gt 0 ]; } || { [ "$want" = absent ] && [ "$n" -eq 0 ]; }; then
    mark=ok
  else
    mark=$([ "$want" = present ] && echo MISSING || echo UNEXPECTED); failures=$((failures + 1))
  fi
  printf '  %-7s %-13s %-10s -> %-13s proto %-3s %-10s %2s  %-10s %s\n' "$want" "$r" "$s" "$d" "$p" "$t" "$n" "$mark" "$why"
done

[ "$failures" -eq 0 ] || die "$failures expectation(s) failed"
log "All protocol expectations met"
