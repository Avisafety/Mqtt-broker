#!/bin/sh
set -e

mkdir -p /mosquitto/data

# Bootstrap: internal bridge user only. credsync.py replaces this file with the
# full set (bridge user + one user per company credential set) as soon as it
# has fetched the credential feed from AviSafe.
mosquitto_passwd -b -c /mosquitto/data/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"

if [ ! -f /mosquitto/data/acl ]; then
  cat > /mosquitto/data/acl <<EOF
user $MQTT_USERNAME
topic readwrite #
EOF
fi

chown -R mosquitto:mosquitto /mosquitto/data
chmod 600 /mosquitto/data/passwd /mosquitto/data/acl

# Keep company credentials + per-serial ACLs in sync in the background.
# Always started: when it is not configured it keeps logging an "ADVARSEL:" line
# on every attempt instead of a single startup message that drowns in the log.
python3 -u /app/credsync.py &

exec mosquitto -c /mosquitto/config/mosquitto.conf
