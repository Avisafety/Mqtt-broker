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
fly secrets set MQTT_USERNAME=... MQTT_PASSWORD=...        # kun intern bridge-bruker
fly secrets set SUPABASE_URL=https://<prosjekt>.supabase.co
fly secrets set SUPABASE_SERVICE_ROLE_KEY=...
fly secrets set AVISAFE_CREDENTIALS_URL=https://<ref>.functions.supabase.co/mqtt-broker-credentials
fly secrets set MQTT_BROKER_API_SECRET=...                 # samme verdi som i Supabase
# valgfritt
fly secrets set MQTT_BRIDGE_HOST=mqtt-broker-avisafe.internal MQTT_BRIDGE_PORT=1883
fly secrets set CRED_SYNC_INTERVAL=300 LOG_LEVEL=DEBUG
```

MQTT_USERNAME / MQTT_PASSWORD brukes nå kun av brua internt – kundene har egne
brukernavn/passord per selskapsgruppe (se `credsync.py`).
SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY peker på hoved-AviSafe-prosjektet.

## Credential-sync (`credsync.py`)

Kjører i bakgrunnen i mosquitto-maskinen (startes av `entrypoint.sh`):

- Poller `mqtt-broker-credentials` (header `x-broker-secret`) hvert
  `CRED_SYNC_INTERVAL`. sekund.
- Skriver `/mosquitto/data/passwd` (bridge-bruker + én bruker per selskapsgruppe)
  og `/mosquitto/data/acl`, der hver bruker kun får publisere på
  `sys/product/{sn}/#` og `thing/product/{sn}/#` for egne/autoriserte serienumre.
- Sender SIGHUP til mosquitto ved endring. Tomt svar → filene beholdes uendret.

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
- `thing/product/{sn}/osd` → én rad (upsert på `sn`) i `flighthub2_positions`.
- sn slås opp i `drones.serienummer` (treff caches 5 min, bom kun 30 s) for
  `drone_id` og `company_id`. Ukjent sn → raden forkastes og bridgen logger
  `ALERT unresolved_sn sn=... dropped_messages=N` (første gang og hver 50.).

### Debug

- `LOG_LEVEL=DEBUG` logger hele JSON-en for hver OSD-melding.
- Hvert 60. sekund skrives en statuslinje per serienummer:
  `status sn=... received=N stored=N dropped_unknown_sn=N write_failed=N`,
  pluss et eksempel på posisjonen for serienumre uten treff i registeret.
- Første vellykkede skriving per serienummer logges eksplisitt.
- Feil skilles: `DATABASE_ERROR` (Supabase svarte med feil) vs
  `NETWORK_ERROR` (fikk ikke kontakt) vs ukjent serienummer.

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
