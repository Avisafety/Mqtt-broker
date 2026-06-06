#!/bin/sh

if [ ! -f /mosquitto/config/passwd ]; then
  mosquitto_passwd -b -c /mosquitto/config/passwd dji Test123456!
fi

exec mosquitto -c /mosquitto/config/mosquitto.conf
