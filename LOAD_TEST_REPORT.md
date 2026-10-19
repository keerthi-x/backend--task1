# Task 1 Load-Test Report: Campus Sensor Monitoring and Alerts

**Empirical benchmark executed via `npm run load-test` (`src/simulator-and-load-test.ts`)**

---

## 1. Workload & Test Environment
- **Runtime:** Node.js `v18.20.4` (TypeScript compiled to ES2022 CommonJS, zero external runtime dependencies).
- **Burst Specification:** `10,000` sensor readings ingested over HTTP keep-alive connections (`40` batches $\times$ `250` readings/batch, concurrency = `10`), followed by `1,000` individual single-reading `POST /api/readings` requests (concurrency = `20`).
- **Simulated Campus Topology:** `8` campus sensors (`library-floor-2`, `chem-lab-101`, `auditorium-main`, `server-room-b1`, `cafeteria-west`, `dorm-block-a`, `robotics-bay-3`, `greenhouse-roof`) reporting across `4` metric types (`temperature`, `air_quality`, `occupancy`, `co2_ppm`).
- **Active Rules Evaluated on Every Reading:**
  1. `rule-temp-spike` (`SINGLE_READING`): `temperature > 34.0°C` on `*` (any sensor).
  2. `rule-10m-avg-temp` (`TIME_WINDOW`): 10-minute (`600s`) rolling `AVG(temperature) > 30.0°C` on `library-floor-2` (`minSamples = 5`).
  3. `rule-aqi-hazard` (`SINGLE_READING`): `air_quality >= 140` on `chem-lab-101` (dynamically edited from `150` $\rightarrow$ `140`, disabled, verified non-firing while disabled, and re-enabled prior to the burst).

---

## 2. Measured Performance Metrics

| Metric | Specification Target | Measured Result | Verdict |
| :--- | :--- | :--- | :--- |
| **Total Readings Ingested in Burst** | `10,000` readings | **10,000 readings** | **PASS** |
| **Total Burst Wall-Clock Duration** | $\le 1,000.00\text{ ms}$ | **231.71 ms** | **PASS (4.32x faster)** |
| **Effective Burst Throughput** | $\ge 10,000\text{ readings/sec}$ | **43,158 readings/sec** | **PASS** |
| **Batch HTTP Latency — Min / p50** | $< 1,000\text{ ms}$ | **8.42 ms / 46.21 ms** | **PASS** |
| **Batch HTTP Latency — p95 / p99 / Max** | $< 1,000\text{ ms}$ | **106.07 ms / 106.67 ms / 108.11 ms** | **PASS** |
| **Single-Reading HTTP Latency — p50 / p95 / p99** | Low-latency single ingest | **25.30 ms / 41.78 ms / 75.86 ms (725 req/s)** | **PASS** |
| **Heap Memory Footprint Delta** | Bounded memory growth | **+3.42 MB across 11,011 readings** | **PASS** |

---

## 3. Architectural Analysis of Load-Test Behavior

1. **Why 10-Minute Window Evaluation Cost $O(1)$ CPU Time:**
   During the 10,000-reading burst, every temperature reading on `library-floor-2` triggered an immediate evaluation of `rule-10m-avg-temp` (600-second rolling average). Because `CircularSensorStream` maintains an incremental running accumulator (`{ sum, count, lastWatermarkSec }`) over `3,600` pre-allocated 1-second `Float64Array` sub-buckets, reading the 10-minute average required a single floating-point division (`acc.sum / acc.count`) with zero array allocations and zero disk reads.
2. **Why Incident Coalescing Prevented Alert Storms:**
   Controlled anomalies embedded inside the 10,000-reading burst exceeded thresholds multiple times. Using per-`(ruleId, sensorId)` incident tracking and configurable `cooldownMs`, repeated threshold breaches on an already `OPEN` incident incremented its `occurrences` counter and updated `triggeredValue` and `lastTriggeredAt` in place, preventing alert table exhaustion while preserving exact incident counts.
