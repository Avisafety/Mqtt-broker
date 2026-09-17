#!/usr/bin/env python3
"""AviSafe DJI Cloud API bridge.

Runs alongside mosquitto in the same Fly app. Responsibilities:
  1. Ack the DJI aircraft handshake (update_topo) so it proceeds to streaming.
  2. Forward OSD telemetry into the main AviSafe Supabase project
     (table: flighthub2_positions) via the REST API.

Configuration comes from environment variables / Fly secrets:
  MQTT_USERNAME, MQTT_PASSWORD          - same credentials mosquitto uses
  MQTT_BRIDGE_HOST (optional)           - default
                                          mosquitto.process.mqtt-broker-avisafe.internal
                                          (app-wide .internal resolves to every
                                          machine, including this one, which has
                                          no listener -> connection refused)
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
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("dji-bridge")

MQTT_HOST = os.environ.get(
    "MQTT_BRIDGE_HOST", "mosquitto.process.mqtt-broker-avisafe.internal"
)
MQTT_PORT = int(os.environ.get("MQTT_BRIDGE_PORT", "1883"))
if MQTT_PORT == 8883:
    # 8883 only exists as TLS termination at Fly's edge - nothing listens on it
    # inside the private network, which shows up as "Connection refused".
    log.warning(
        "MQTT_BRIDGE_PORT=8883 is not reachable internally (TLS is terminated at "
        "Fly's edge) - falling back to 1883 for the internal connection"
    )
    MQTT_PORT = 1883
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""

ORDER_ID = "dji-cloud"
# Negative lookups are cached only briefly so a newly registered drone starts
# working within seconds instead of minutes.
DRONE_CACHE_TTL = 300  # seconds (successful lookups)
DRONE_NEGATIVE_CACHE_TTL = 30  # seconds (serial number not found)
STATS_INTERVAL = 60  # seconds between summary lines

# sn -> (expires_at, {"drone_id": ..., "company_id": ...} | None)
_drone_cache = {}
# sn -> number of dropped messages because the sn is unknown
_dropped_counts = {}
# sn -> counters, plus the last payload seen for unresolved serials
_stats = {}
_unresolved_samples = {}
_stored_once = set()
_last_stats_at = 0.0
_no_position_counts = {}


def _bump(sn, key):
    row = _stats.setdefault(
        sn, {"received": 0, "stored": 0, "dropped": 0, "failed": 0, "no_position": 0}
    )
    row[key] += 1


def maybe_print_stats():
    """Print a short summary line per serial number every STATS_INTERVAL."""
    global _last_stats_at
    now = time.time()
    if now - _last_stats_at < STATS_INTERVAL:
        return
    _last_stats_at = now
    if not _stats:
        log.info("status: no messages received in the last %ds", STATS_INTERVAL)
        return
    for sn, row in _stats.items():
        log.info(
            "status sn=%s received=%d stored=%d dropped_unknown_sn=%d "
            "write_failed=%d no_position=%d",
            sn,
            row["received"],
            row["stored"],
            row["dropped"],
            row["failed"],
            row["no_position"],
        )
    for sn, sample in _unresolved_samples.items():
        log.warning(
            "status unresolved_sn=%s sample_position=%s "
            "(register this serial number on a drone in AviSafe)",
            sn,
            json.dumps(sample),
        )

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
                # Match on either the external serial number or the internal one
                "or": "(serienummer.eq.{0},internal_serial.eq.{0})".format(sn),
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
            log.error(
                "drone lookup DATABASE_ERROR sn=%s status=%s body=%s",
                sn,
                resp.status_code,
                resp.text[:300],
            )
            return None  # transient failure: do not cache
    except Exception as exc:  # noqa: BLE001
        log.error("drone lookup NETWORK_ERROR sn=%s error=%s", sn, exc)
        return None

    ttl = DRONE_CACHE_TTL if result else DRONE_NEGATIVE_CACHE_TTL
    _drone_cache[sn] = (now + ttl, result)
    if result:
        log.info("resolved sn=%s drone_id=%s company_id=%s", sn, result["drone_id"], result["company_id"])
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


def note_no_position(sn):
    count = _no_position_counts.get(sn, 0) + 1
    _no_position_counts[sn] = count
    if count == 1 or count % 50 == 0:
        log.warning(
            "ALERT no_position sn=%s count=%d "
            "reason=osd_frame_without_latitude_longitude_in_data_or_data_host",
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

    _bump(sn, "received")
    log.debug("OSD sn=%s payload=%s", sn, json.dumps(payload))

    # Some OSD frames carry position fields directly under `data`; compact
    # gateway/host frames instead nest everything under `data.host`. Fall
    # back to `data.host` whenever `data` itself has no usable coordinates.
    host = data.get("host")
    source = data
    if (data.get("latitude") is None or data.get("longitude") is None) and isinstance(host, dict):
        source = host

    lat = num(source.get("latitude"))
    lng = num(source.get("longitude"))
    if lat is None or lng is None:
        _bump(sn, "no_position")
        note_no_position(sn)
        log.debug("OSD sn=%s has no position fields, skipping", sn)
        return  # no position in this OSD frame

    drone = resolve_drone(sn)
    if not drone:
        _bump(sn, "dropped")
        _unresolved_samples[sn] = {
            "sn": sn,
            "lat": lat,
            "lng": lng,
            "height": num(source.get("height")),
            "battery_percent": num(source.get("capacity_percent")),
        }
        note_unresolved(sn)
        return

    height = num(source.get("height"))
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
        "altitude_m": num(source.get("elevation")) if num(source.get("elevation")) is not None else num(source.get("altitude")),
        "vert_speed_ms": num(source.get("vertical_speed")),
        "ground_speed_ms": num(source.get("horizontal_speed")),
        "course_deg": num(source.get("attitude_head")),
        "raw": payload,
    }

    try:
        resp = session.post(
            SUPABASE_URL + "/rest/v1/flighthub2_positions?on_conflict=sn",
            headers=supabase_headers(
                {"Prefer": "resolution=merge-duplicates,return=minimal"}
            ),
            data=json.dumps(row),
            timeout=10,
        )
        if resp.status_code >= 300:
            _bump(sn, "failed")
            log.error(
                "write DATABASE_ERROR sn=%s status=%s body=%s row=%s",
                sn,
                resp.status_code,
                resp.text[:300],
                json.dumps({k: v for k, v in row.items() if k != "raw"}),
            )
        else:
            _bump(sn, "stored")
            _unresolved_samples.pop(sn, None)
            if sn not in _stored_once:
                _stored_once.add(sn)
                log.info(
                    "first position stored sn=%s lat=%s lng=%s height=%s",
                    sn,
                    lat,
                    lng,
                    height,
                )
            log.debug("stored sn=%s lat=%s lng=%s height=%s", sn, lat, lng, height)
    except Exception as exc:  # noqa: BLE001
        _bump(sn, "failed")
        log.error("write NETWORK_ERROR sn=%s error=%s", sn, exc)


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

    maybe_print_stats()


def main():
    log.info(
        "bridge starting: connecting to %s:%s as user=%s",
        MQTT_HOST,
        MQTT_PORT,
        MQTT_USERNAME or "(unset)",
    )
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
