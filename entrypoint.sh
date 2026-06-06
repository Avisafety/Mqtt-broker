#!/bin/sh
set -e

mkdir -p /mosquitto/data

if [ ! -f /mosquitto/data/passwd ]; then
  mosquitto_passwd -b -c /mosquitto/data/passwd dji 'Test123456!'
fi

chown -R mosquitto:mosquitto /mosquitto/data
chmod 644 /mosquitto/data/passwd

exec mosquitto -c /mosquitto/config/mosquitto.conf