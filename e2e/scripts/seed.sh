#!/usr/bin/env bash
# Replaces the flow DB's events with seed/event_logs.csv (see seed-export.sh),
# so Traffic Events has data without generating traffic. Hostnames, policy
# and resource names are mapped onto this lab's current keys, IPs and IDs;
# events get fresh IDs, and all times shift so the newest arrived just now.
# The fixture's peers must be registered in management (`make peers` once
# after `make clean`), but they don't need to be running.
cd "$(dirname "$0")/.."
source scripts/lib.sh

IN=${IN:-seed/event_logs.csv}
[ -f "$IN" ] || die "$IN not found — run 'make seed-export' against a lab with traffic first"
TOKEN=$(get_token)
mapping=$(lab_mapping_sql)

log "Seeding the flow DB from $IN"
{
  echo "BEGIN;"
  echo "$mapping"
  cat <<'SQL'
CREATE TEMP TABLE seed (
  event_time timestamptz, created_at timestamptz, reporter text, flow text, type text, rule text,
  source_resource text, dest_resource text, direction text, protocol text, source_ip text, dest_ip text,
  conn_source_port bigint, conn_dest_port bigint, icmp_type bigint, icmp_code bigint,
  rx_packets bigint, tx_packets bigint, rx_bytes bigint, tx_bytes bigint);
COPY seed FROM STDIN WITH CSV HEADER;
SQL
  cat "$IN"
  echo '\.'
  cat <<'SQL'
DO $$
DECLARE missing text;
BEGIN
  SELECT string_agg(DISTINCT h, ', ') INTO missing FROM (
    SELECT reporter AS h FROM seed
    UNION SELECT substr(source_ip, 6) FROM seed WHERE source_ip LIKE 'peer:%'
    UNION SELECT substr(dest_ip, 6) FROM seed WHERE dest_ip LIKE 'peer:%') hosts
  WHERE h NOT IN (SELECT hostname FROM m_peer WHERE key <> '');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'peers not registered in management: % (run make peers once)', missing;
  END IF;
END $$;

TRUNCATE event_logs, session_records, etl_watermarks RESTART IDENTITY;

WITH shift AS (SELECT now() - max(created_at) AS d FROM seed),
     flows AS (SELECT reporter, flow, gen_random_uuid()::text AS new_flow
               FROM (SELECT DISTINCT reporter, flow FROM seed) f)
INSERT INTO event_logs (created_at, updated_at, event_time, event_id, public_key, flow_id, type,
  rule_id, source_resource_id, dest_resource_id, direction, protocol, source_ip, dest_ip,
  conn_source_port, conn_dest_port, icmp_icmp_type, icmp_icmp_code,
  rx_packets, tx_packets, rx_bytes, tx_bytes)
SELECT s.created_at + shift.d, s.created_at + shift.d, s.event_time + shift.d,
       gen_random_uuid()::text, p.key, f.new_flow, s.type,
       coalesce(r.id, ''), coalesce(sr.id, ''), coalesce(dr.id, ''), s.direction, s.protocol,
       coalesce(sp.ip, s.source_ip), coalesce(dp.ip, s.dest_ip),
       s.conn_source_port, s.conn_dest_port, s.icmp_type, s.icmp_code,
       s.rx_packets, s.tx_packets, s.rx_bytes, s.tx_bytes
FROM seed s
CROSS JOIN shift
JOIN flows f USING (reporter, flow)
JOIN m_peer p ON p.hostname = s.reporter
LEFT JOIN m_rule r ON s.rule <> '' AND r.name = s.rule
LEFT JOIN m_res sr ON s.source_resource <> '' AND sr.name = s.source_resource
LEFT JOIN m_res dr ON s.dest_resource <> '' AND dr.name = s.dest_resource
LEFT JOIN m_peer sp ON s.source_ip = 'peer:' || sp.hostname
LEFT JOIN m_peer dp ON s.dest_ip = 'peer:' || dp.hostname
ORDER BY s.created_at, s.event_time;

SELECT format('    %s events, %s flows, %s to %s; %s rules and %s resources no longer exist',
  count(*), count(DISTINCT (public_key, flow_id)),
  to_char(min(event_time), 'HH24:MI:SS'), to_char(max(event_time), 'HH24:MI:SS'),
  (SELECT count(*) FROM seed s WHERE s.rule <> '' AND s.rule NOT IN (SELECT name FROM m_rule)),
  (SELECT count(*) FROM seed s WHERE (s.source_resource <> '' AND s.source_resource NOT IN (SELECT name FROM m_res))
                                  OR (s.dest_resource <> '' AND s.dest_resource NOT IN (SELECT name FROM m_res))))
FROM event_logs;
COMMIT;
SQL
} | "${PSQL[@]}" -At || die "Seeding failed, the flow DB is unchanged"

log "Restarting the ETL so it rebuilds sessions from the seeded events now"
docker compose restart etl >/dev/null
log "Done — open Traffic Events in the dashboard"
