#!/bin/sh
# Starts the listeners named in $LISTEN ("http:8000 tcp:5432 udp:514"), then
# hands over to NetBird's own entrypoint. Each listener answers with
# "<hostname>:<proto>/<port>" so traffic.sh can tell what it reached.
#   http  also serves /big.bin (2 MiB) for slow, long-lived downloads
#   tcp   sends the banner, then echoes until the client closes (long sessions)
#   udp   answers every datagram with the banner
#   sctp  like tcp, over SCTP
set -e
name=$(hostname)
for spec in $LISTEN; do
  proto=${spec%%:*}
  port=${spec#*:}
  case "$proto" in
    http)
      mkdir -p "/srv/$port"
      echo "$name:http/$port" > "/srv/$port/index.html"
      head -c 2097152 /dev/urandom > "/srv/$port/big.bin"
      busybox-extras httpd -p "$port" -h "/srv/$port"
      ;;
    tcp)
      socat "TCP-LISTEN:$port,fork,reuseaddr" "SYSTEM:echo $name\:tcp/$port; exec cat" &
      ;;
    udp)
      socat "UDP-RECVFROM:$port,fork" "SYSTEM:echo $name\:udp/$port" &
      ;;
    sctp)
      socat "SCTP-LISTEN:$port,fork,reuseaddr" "SYSTEM:echo $name\:sctp/$port; exec cat" &
      ;;
    *)
      echo "unknown listener '$spec'" >&2
      exit 1
      ;;
  esac
  echo "listening on $proto/$port"
done
exec /usr/local/bin/netbird-entrypoint.sh "$@"
