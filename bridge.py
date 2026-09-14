#!/usr/bin/env python3
"""AviSafe DJI Cloud API bridge.

Runs alongside mosquitto in the same Fly app. Responsibilities:
  1. Ack the DJI aircraft handshake (update_topo) so it proceeds to streaming.
  2. Forward OSD telemetry into the main AviSafe Supabase project
     (table: flighthub2_positions) via the REST API.

Configuration comes from environment variables / Fly secrets:
  MQTT_USERNAME, MQTT_PASSWORD          - same credentials mosquitto uses
  MQTT_BRIDGE_HOST (optional)           - default mqtt-broker-avisafe.internal
  MQTT_BRIDGE_PORT (optional)           - default 1883
  SUPABASE_URL                          - main AviSafe project URL
  SUPABASE_SERVICE_ROLE_KEY             - service role key for that project
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("dji-bridge")

MQTT_HOST = os.environ.get("MQTT_BRIDGE_HOST", "mqtt-broker-avisafe.internal")
MQTT_PORT = int(os.environ.get("MQTT_BRIDGE_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""

ORDER_ID = "dji-cloud"
DRONE_CACHE_TTL = 300  # seconds

# sn -> (expires_at, {"drone_id": ..., "company_id": ...} | None)
_drone_cache = {}
# sn -> number of dropped messages because the sn is unknown
_dropped_counts = {}

session = requests.Session()


def supabase_headers(extra=None):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": "Bearer " + SUPABASE_KEY,
        "Content-Type": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def resolve_drone(sn):
    """Return {"drone_id", "company_id"} for a serial number, or None."""
    now = time.time()
    cached = _drone_cache.get(sn)
    if cached and cached[0] > now:
        return cached[1]

    result = None
    try:
        resp = session.get(
            SUPABASE_URL + "/rest/v1/drones",
            params={
                "serienummer": "eq." + sn,
                "select": "id,company_id",
                "limit": "1",
            },
            headers=supabase_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            rows = resp.json()
            if rows:
                result = {"drone_id": rows[0]["id"], "company_id": rows[0]["company_id"]}
        else:
            log.error("drone lookup failed for %s: %s %s", sn, resp.status_code, resp.text[:300])
            return None  # transient failure: do not cache
    except Exception as exc:  # noqa: BLE001
        log.error("drone lookup error for %s: %s", sn, exc)
        return None

    _drone_cache[sn] = (now + DRONE_CACHE_TTL, result)
    return result


def note_unresolved(sn):
    count = _dropped_counts.get(sn, 0) + 1
    _dropped_counts[sn] = count
    if count == 1 or count % 50 == 0:
        log.warning(
            "ALERT unresolved_sn sn=%s dropped_messages=%d "
            "reason=serial_number_not_found_in_drones_table",
            sn,
            count,
        )


def to_iso(value):
    """Translate a DJI OSD timestamp (ms or s epoch) to ISO-8601, else None."""
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    seconds = value / 1000.0 if value > 1e11 else float(value)
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def num(value):
    if isinstance(value, bool):
        return None
    return value if isinstance(value, (int, float)) else None


def handle_update_topo(client, sn, payload):
    reply = {
        "tid": payload.get("tid"),
        "bid": payload.get("bid"),
        "timestamp": int(payload.get("timestamp") or 0) + 2,
        "method": "update_topo",
        "data": {"result": 0},
    }
    topic = "sys/product/{}/status_reply".format(sn)
    client.publish(topic, json.dumps(reply), qos=0)
    log.info("acked update_topo for %s on %s", sn, topic)


def handle_osd(sn_from_topic, payload):
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return

    sn = payload.get("gateway") or sn_from_topic
    if not sn:
        log.warning("OSD message without gateway/sn, skipping")
        return

    lat = num(data.get("latitude"))
    lng = num(data.get("longitude"))
    if lat is None or lng is None:
        return  # no position in this OSD frame

    drone = resolve_drone(sn)
    if not drone:
        note_unresolved(sn)
        return

    height = num(data.get("height"))
    row = {
        "company_id": drone["company_id"],
        "drone_id": drone["drone_id"],
        "order_id": ORDER_ID,
        "sn": sn,
        "flight_status": "inflight" if (height is not None and height > 0) else "ground",
        "time_stamp": to_iso(payload.get("timestamp")) or datetime.now(timezone.utc).isoformat(),
        "lat": lat,
        "lng": lng,
        "height_m": height,
        "altitude_m": num(data.get("elevation")) if num(data.get("elevation")) is not None else num(data.get("altitude")),
        "vert_speed_ms": num(data.get("vertical_speed")),
        "ground_speed_ms": num(data.get("horizontal_speed")),
        "course_deg": num(data.get("attitude_head")),
        "raw": payload,
    }

    try:
        resp = session.post(
            SUPABASE_URL + "/rest/v1/flighthub2_positions",
            headers=supabase_headers({"Prefer": "return=minimal"}),
            data=json.dumps(row),
            timeout=10,
        )
        if resp.status_code >= 300:
            log.error("insert failed for %s: %s %s", sn, resp.status_code, resp.text[:300])
    except Exception as exc:  # noqa: BLE001
        log.error("insert error for %s: %s", sn, exc)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc != 0:
        log.error("MQTT connect failed rc=%s", rc)
        return
    log.info("connected to %s:%s", MQTT_HOST, MQTT_PORT)
    client.subscribe([("sys/#", 0), ("thing/#", 0)])


def on_disconnect(client, userdata, rc, properties=None, reason=None):
    log.warning("disconnected from broker rc=%s - reconnecting", rc)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:  # noqa: BLE001
        log.debug("non-JSON message on %s", msg.topic)
        return
    if not isinstance(payload, dict):
        return

    parts = msg.topic.split("/")
    # sys/product/{sn}/status  |  thing/product/{sn}/osd
    sn = parts[2] if len(parts) >= 4 else None
    leaf = parts[-1]

    try:
        if leaf == "status" and payload.get("method") == "update_topo" and sn:
            handle_update_topo(client, sn, payload)
        elif leaf == "osd":
            handle_osd(sn, payload)
    except Exception as exc:  # noqa: BLE001
        log.exception("error handling message on %s: %s", msg.topic, exc)


def main():
    missing = [
        name
        for name, value in (
            ("MQTT_USERNAME", MQTT_USERNAME),
            ("MQTT_PASSWORD", MQTT_PASSWORD),
            ("SUPABASE_URL", SUPABASE_URL),
            ("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_KEY),
        )
        if not value
    ]
    if missing:
        log.error("missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    try:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="avisafe-bridge",
            protocol=mqtt.MQTTv311,
        )
    except AttributeError:  # paho-mqtt 1.x
        client = mqtt.Client(client_id="avisafe-bridge", protocol=mqtt.MQTTv311)

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as exc:  # noqa: BLE001
            log.error("broker connection error: %s - retrying in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
