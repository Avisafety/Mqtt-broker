#!/bin/sh
set -e

mkdir -p /mosquitto/data

mosquitto_passwd -b -c /mosquitto/data/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"

chown -R mosquitto:mosquitto /mosquitto/data
chmod 600 /mosquitto/data/passwd

exec mosquitto -c /mosquitto/config/mosquitto.conf
