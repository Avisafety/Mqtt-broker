# mqtt-broker-avisafe

Mosquitto MQTT-broker på Fly.io, brukt av DJI Pilot 2-skytilkoblingen (`/dji`),
pluss en Python-bridge som svarer på DJI Cloud API-handshaken og skriver
telemetri inn i AviSafe-databasen.

## Prosesser (Fly `[processes]`)

| Prosess | Kommando | Rolle |
|---|---|---|
| `mosquitto` | `/entrypoint.sh` | Brokeren. Eneste prosess som eksponeres utad (1883 / 8883 TLS). |
| `bridge` | `python3 /app/bridge.py` | Kobler til brokeren over Flys interne nett, acker handshake, skriver posisjoner. |

Fly kjører hver prosess i egen maskin, så bridgen kobler til
`mqtt-broker-avisafe.internal:1883` (ikke `localhost`).

## Secrets

```
fly secrets set MQTT_USERNAME=... MQTT_PASSWORD=...
fly secrets set SUPABASE_URL=https://<prosjekt>.supabase.co
fly secrets set SUPABASE_SERVICE_ROLE_KEY=...
# valgfritt
fly secrets set MQTT_BRIDGE_HOST=mqtt-broker-avisafe.internal MQTT_BRIDGE_PORT=1883
```

MQTT_USERNAME / MQTT_PASSWORD må holdes i sync med Supabase-secrets med samme
navn (brukes av edge-funksjonen `pilot-cloud-config`).
SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY peker på hoved-AviSafe-prosjektet.

## Deploy

```
fly deploy
fly logs -a mqtt-broker-avisafe
```

## Bridge-oppførsel

- Abonnerer på `sys/#` og `thing/#`.
- `sys/product/{sn}/status` med `method: "update_topo"` → svar på
  `sys/product/{sn}/status_reply` med samme `tid`/`bid`, `timestamp + 2` og
  `data: {"result": 0}`.
- `thing/product/{sn}/osd` → én rad i `flighthub2_positions`.
- sn slås opp i `drones.serienummer` (5 min cache) for `drone_id` og
  `company_id`. Ukjent sn → raden forkastes og bridgen logger
  `ALERT unresolved_sn sn=... dropped_messages=N` (første gang og hver 50.).

## Feltmapping (`flighthub2_positions`)

| Kolonne | Kilde |
|---|---|
| company_id | `drones.company_id` via sn |
| drone_id | `drones.id` via sn |
| order_id | `"dji-cloud"` |
| sn | OSD `gateway` (fallback: sn i topic) |
| flight_status | `"inflight"` hvis `height > 0`, ellers `"ground"` |
| time_stamp | OSD `timestamp` (ms/s epoch) eller nå |
| lat / lng | `data.latitude` / `data.longitude` |
| height_m | `data.height` |
| altitude_m | `data.elevation` eller `data.altitude` |
| vert_speed_ms | `data.vertical_speed` |
| ground_speed_ms | `data.horizontal_speed` |
| course_deg | `data.attitude_head` |
| raw | hele meldingen (jsonb) |
| height_type, remote_id_status, coordinate_system, uas_*, mission_id | null |
