# Task 1: Campus Sensor Monitoring and Alerts
## 1. Prerequisites, Installation & Configuration

### Prerequisites
- **Node.js** `>= 18.0.0` (tested on Node.js `v18.20.4`)
- No external database daemon (Redis, PostgreSQL, InfluxDB) is required; durability is provided by a custom IEEE 802.3 CRC32-checksummed Write-Ahead Log (`src/shared/wal-engine.ts`).

### Installation & Build
```bash
# Clone the repository
git clone <YOUR_TASK_1_GITHUB_REPO_URL>
cd task1-campus-sensor-monitoring-ts

# Compile TypeScript source files (src/**/*.ts -> dist/**/*.js)
npm run build
```

### Configuration (Environment Variables)
| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `4001` | HTTP & Server-Sent Events (SSE) listening port |
| `WAL_PATH` | `./data/task1-sensor-telemetry.wal.ndjson` | Path to the append-only CRC32 Write-Ahead Log file |

---

## 2. Execution & Testing Instructions

### Start the Sensor Monitoring Backend Server
```bash
npm start
# Listening on http://localhost:4001
```

### Run the Campus Sensor Simulator & 10,000-Reading/Sec Burst Load Test
```bash
npm test
# Alias: npm run load-test or npm run simulator
```
This single command boots an isolated server instance, tests rule creation/editing/enabling/disabling, verifies the exact problem-statement example reading (`library-floor-2`, `34.5°C`), executes a **10,000-reading burst in under 1 second** (`231.71 ms` = **`43,158 readings/sec`**), tests `OPEN -> ACKNOWLEDGED -> RESOLVED` alert transitions, and writes the full JSON telemetry report to `data/task1-benchmark-results.json`.

---

## 3. REST API Reference & Example Payloads

### 3.1 Ingest Individual Reading (`POST /api/readings`)
```bash
curl -X POST http://localhost:4001/api/readings \
  -H "Content-Type: application/json" \
  -d '{
    "sensorId": "library-floor-2",
    "type": "temperature",
    "value": 34.5,
    "timestamp": "2026-10-12T10:30:00Z"
  }'
```

### 3.2 Ingest Batch of Readings (`POST /api/readings/batch` or `POST /api/readings`)
```bash
curl -X POST http://localhost:4001/api/readings/batch \
  -H "Content-Type: application/json" \
  -d '{
    "readings": [
      { "sensorId": "library-floor-2", "type": "temperature", "value": 34.5, "timestamp": "2026-10-12T10:30:00Z" },
      { "sensorId": "chem-lab-101", "type": "air_quality", "value": 152.0, "timestamp": "2026-10-12T10:30:01Z" }
    ]
  }'
```

### 3.3 Organizer Alert Rules Management (`POST / GET / PATCH / enable / disable`)
- **Create a Single-Reading Rule (`POST /api/rules`)**:
  ```json
  {
    "id": "rule-temp-spike",
    "name": "Critical Temperature Spike",
    "sensorId": "*",
    "metricType": "temperature",
    "kind": "SINGLE_READING",
    "operator": ">",
    "threshold": 34.0,
    "cooldownMs": 60000
  }
  ```
- **Create a 10-Minute Rolling Time-Window Rule (`POST /api/rules`)**:
  ```json
  {
    "id": "rule-10m-avg-temp",
    "name": "Sustained 10-Minute High Temperature Average",
    "sensorId": "library-floor-2",
    "metricType": "temperature",
    "kind": "TIME_WINDOW",
    "windowSeconds": 600,
    "aggregator": "AVG",
    "operator": ">",
    "threshold": 30.0,
    "minSamples": 5,
    "cooldownMs": 60000
  }
  ```
- **Edit Rule:** `PATCH /api/rules/:id` (e.g. `{ "threshold": 32.5 }`)
- **Enable / Disable Rule:** `POST /api/rules/:id/enable` and `POST /api/rules/:id/disable`
- **List Rules:** `GET /api/rules`

### 3.4 Organizer Alert Lifecycle (`GET / acknowledge / resolve`)
- **View Alerts:** `GET /api/alerts?status=OPEN&sensorId=library-floor-2`
- **Acknowledge Alert:** `POST /api/alerts/:id/acknowledge` (`{ "organizer": "alex@gdg.dev", "note": "Checking HVAC" }`)
- **Resolve Alert:** `POST /api/alerts/:id/resolve` (`{ "organizer": "alex@gdg.dev", "resolutionNote": "Chiller restored" }`)

---

## 4. Architecture & Throughput / Reliability Trade-Offs

### 4.1 Custom Primitive #1: $O(1)$ Circular 1-Second Sub-Bucket Sliding Window (`src/sliding-window.ts`)
- **Problem:** Executing `SELECT AVG(value) FROM readings WHERE timestamp >= NOW() - 10m` or scanning an in-memory array on every incoming reading requires $O(N^2) \approx 10^8$ operations during a 10,000-reading/sec burst.
- **Solution:** `CircularSensorStream` pre-allocates `3,600` 1-second circular sub-buckets (covering 60 minutes) in flat `Float64Array` and `Uint32Array` buffers (`bucketEpochSec`, `bucketCount`, `bucketSum`, `bucketMin`, `bucketMax`).
- **Amortized $O(1)$ Window Evaluation:** Active window rules (such as `600s` = 10 minutes) maintain a running accumulator `{ sum, count, lastWatermarkSec }`. Each incoming reading updates its 1-second slot (`epochSec % 3600`) and increments the accumulator in $O(1)$ time. When the event-time watermark advances by $\Delta t$ seconds, only the $\Delta t$ expired 1-second sub-buckets sliding off the tail are subtracted.
- **Welford's Online Anomaly Detection:** Simultaneously maintains running mean and variance ($M_2$) via Welford's recurrence in $O(1)$ time, attaching a statistical `anomalyZScore` ($|x - \mu|/\sigma$) to every triggered alert.

### 4.2 Custom Primitive #2: String-Interned TypedArray Ring Buffer & Group-Commit WAL (`src/engine.ts`, `src/shared/wal-engine.ts`)
- **Zero-GC Telemetry Retention:** Interns repeated `sensorId` and `type` strings into 16-bit integer IDs (`Uint16Array`) and writes values/timestamps into contiguous `Float64Array` slots (`65,536` capacity), eliminating V8 garbage-collection pauses during 10k/s bursts.
- **Group-Commit Write-Behind Persistence:** Rather than blocking the HTTP event loop on 10,000 synchronous `fsync` calls per second, readings are queued into `DurableWalEngine.enqueueWriteBehind`, which flushes coalesced batches (up to `2,500` frames or `20ms` windows) protected by IEEE 802.3 CRC32 checksums. Rule mutations and alert state transitions use synchronous writes (`appendSyncFsync`) for immediate durability.

### 4.3 Key Trade-Offs & Limitations
1. **1-Second Sub-Bucket Quantization vs. Exact Millisecond Sliding Windows:** Quantizing time-window buckets to 1-second boundaries bounds memory to a fixed `~187 KB` per sensor stream regardless of burst volume (10 readings/s or 100,000 readings/s), trading sub-second window boundary precision for $O(1)$ speed and bounded RAM.
2. **Group-Commit Write-Behind Window (`20ms`) for Telemetry:** Coalescing telemetry disk writes every `20ms` increases throughput from ~1,000 req/s to **43,158 readings/s**, with the trade-off that a sudden power loss could lose up to `20ms` of raw sensor readings (whereas alert rule changes and alert acknowledgements/resolutions are flushed synchronously).

---

## 5. Empirical Load-Test Summary (10,000 Readings in 1 Second)

| Metric | Target | Measured Result |
| :--- | :--- | :--- |
| **10,000-Reading Burst Wall-Clock Duration** | $\le 1,000.00\text{ ms}$ | **231.71 ms** |
| **Sustained Burst Throughput** | $\ge 10,000\text{ readings/sec}$ | **43,158 readings/sec (4.32x target)** |
| **Batch Request HTTP Latency (`250 readings/batch`)** | Sub-second | **p50: 46.21 ms \| p95: 106.07 ms \| p99: 106.67 ms** |
| **Single-Reading HTTP Latency (`1,000 requests`)** | Low latency | **p50: 25.30 ms \| p95: 41.78 ms \| p99: 75.86 ms** |
| **V8 Heap Delta Across 11,011 Readings** | Bounded memory | **+3.42 MB** |

See [`LOAD_TEST_REPORT.md`](./LOAD_TEST_REPORT.md) for the complete benchmark breakdown.
