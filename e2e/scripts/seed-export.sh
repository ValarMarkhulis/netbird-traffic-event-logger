#!/usr/bin/env bash
# Exports the flow DB's event_logs to seed/event_logs.csv in a lab-independent
# form: public keys and NetBird IPs become peer hostnames ("peer:<hostname>"
# for IPs), rule IDs become "<policy>/<rule>", resource IDs become resource
# names. scripts/seed.sh maps them back onto whatever lab it runs against.
cd "$(dirname "$0")/.."
source scripts/lib.sh

OUT=${OUT:-seed/event_logs.csv}
TOKEN=$(get_token)
mapping=$(lab_mapping_sql)

log "Checking that every event maps onto this lab"
"${PSQL[@]}" -At <<SQL
$mapping
SELECT format('    %s events; unmapped: %s reporters, %s rules, %s resources',
  count(*),
  count(*) FILTER (WHERE p.key IS NULL),
  count(*) FILTER (WHERE e.rule_id <> '' AND r.id IS NULL),
  count(*) FILTER (WHERE (e.source_resource_id <> '' AND sr.id IS NULL) OR (e.dest_resource_id <> '' AND dr.id IS NULL)))
FROM event_logs e
LEFT JOIN m_peer p ON p.key = e.public_key
LEFT JOIN m_rule r ON r.id = e.rule_id
LEFT JOIN m_res sr ON sr.id = e.source_resource_id
LEFT JOIN m_res dr ON dr.id = e.dest_resource_id;
SQL

log "Writing $OUT (events from unmapped reporters are left out)"
"${PSQL[@]}" > "$OUT" <<SQL
$mapping
COPY (
  SELECT e.event_time, e.created_at, p.hostname AS reporter, e.flow_id AS flow, e.type,
         coalesce(r.name, '') AS rule,
         coalesce(sr.name, '') AS source_resource, coalesce(dr.name, '') AS dest_resource,
         e.direction, e.protocol,
         coalesce('peer:' || sp.hostname, e.source_ip) AS source_ip,
         coalesce('peer:' || dp.hostname, e.dest_ip) AS dest_ip,
         e.conn_source_port, e.conn_dest_port, e.icmp_icmp_type AS icmp_type, e.icmp_icmp_code AS icmp_code,
         e.rx_packets, e.tx_packets, e.rx_bytes, e.tx_bytes
  FROM event_logs e
  JOIN m_peer p ON p.key = e.public_key
  LEFT JOIN m_rule r ON r.id = e.rule_id
  LEFT JOIN m_res sr ON sr.id = e.source_resource_id
  LEFT JOIN m_res dr ON dr.id = e.dest_resource_id
  LEFT JOIN m_peer sp ON sp.ip = e.source_ip
  LEFT JOIN m_peer dp ON dp.ip = e.dest_ip
  ORDER BY e.event_time, e.id
) TO STDOUT WITH CSV HEADER;
SQL
log "Exported $(($(wc -l < "$OUT") - 1)) events"
