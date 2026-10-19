"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.TypedTelemetryRingBuffer = exports.SensorMonitoringEngine = void 0;
var crypto = _interopRequireWildcard(require("crypto"));
var _walEngine = require("./shared/wal-engine");
var _slidingWindow = require("./sliding-window");
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function (nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || typeof obj !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
/**
 * Standout Systems Primitive #2:
 * String-Interned Pre-Allocated TypedArray Ring Buffer for High-Throughput Telemetry
 *
 * Allocating 10,000+ JavaScript objects per second on the V8 heap causes frequent
 * Young Generation (Scavenge) GC pauses. By interning `sensorId` and `type` strings into
 * compact 16-bit integer IDs (`Uint16Array`) and storing values and timestamps in
 * contiguous `Float64Array` ring slots, we retain the last `capacity` readings in memory
 * with zero per-reading heap allocation.
 */
class TypedTelemetryRingBuffer {
  capacity;
  sensorIdSlot;
  metricTypeSlot;
  valueSlot;
  timestampMsSlot;
  strToId = new Map();
  idToStr = [];
  writeCursor = 0;
  totalWritten = 0;
  constructor(capacity = 65536) {
    this.capacity = capacity;
    this.sensorIdSlot = new Uint16Array(capacity);
    this.metricTypeSlot = new Uint16Array(capacity);
    this.valueSlot = new Float64Array(capacity);
    this.timestampMsSlot = new Float64Array(capacity);
  }
  intern(str) {
    let id = this.strToId.get(str);
    if (id === undefined) {
      id = this.idToStr.length;
      this.idToStr.push(str);
      this.strToId.set(str, id);
    }
    return id;
  }
  push(sensorId, metricType, value, timestampMs) {
    const idx = this.writeCursor;
    this.sensorIdSlot[idx] = this.intern(sensorId);
    this.metricTypeSlot[idx] = this.intern(metricType);
    this.valueSlot[idx] = value;
    this.timestampMsSlot[idx] = timestampMs;
    this.writeCursor = (idx + 1) % this.capacity;
    this.totalWritten++;
  }
  getRecent(limit = 50) {
    const count = Math.min(limit, this.totalWritten, this.capacity);
    const result = [];
    for (let i = 0; i < count; i++) {
      const idx = (this.writeCursor - 1 - i + this.capacity) % this.capacity;
      result.push({
        sensorId: this.idToStr[this.sensorIdSlot[idx]] || 'unknown',
        type: this.idToStr[this.metricTypeSlot[idx]] || 'unknown',
        value: this.valueSlot[idx],
        timestamp: new Date(this.timestampMsSlot[idx]).toISOString()
      });
    }
    return result;
  }
  getStats() {
    return {
      capacity: this.capacity,
      totalWritten: this.totalWritten,
      internedStringsCount: this.idToStr.length,
      bufferUtilizationPct: Number((Math.min(this.totalWritten, this.capacity) / this.capacity * 100).toFixed(2))
    };
  }
}
exports.TypedTelemetryRingBuffer = TypedTelemetryRingBuffer;
class SensorMonitoringEngine {
  wal;
  ringBuffer;
  streams = new Map();
  rules = new Map();
  // Secondary index by metricType for O(1) candidate rule lookup during 10k/s bursts
  rulesByMetric = new Map();
  alerts = new Map();
  // Active incident lookup keyed by `${ruleId}::${sensorId}` for deduplication & cooldown
  activeIncidentByRuleSensor = new Map();
  lastTriggerMsByRuleSensor = new Map();
  totalReadingsIngested = 0;
  totalSingleRequests = 0;
  totalBatchRequests = 0;
  totalAlertsTriggered = 0;
  sseListeners = new Set();
  constructor(walFilePath) {
    this.wal = new _walEngine.DurableWalEngine(walFilePath);
    this.ringBuffer = new TypedTelemetryRingBuffer(65536);
    this.recoverFromWal();
  }
  recoverFromWal() {
    this.wal.replay(frame => {
      const p = frame.payload;
      switch (frame.op) {
        case 'RULE_UPSERT':
          {
            const rule = p;
            this.rules.set(rule.id, rule);
            this.rebuildRuleIndex();
            break;
          }
        case 'ALERT_UPSERT':
          {
            const alert = p;
            this.alerts.set(alert.id, alert);
            const incKey = `${alert.ruleId}::${alert.sensorId}`;
            if (alert.status === 'OPEN' || alert.status === 'ACKNOWLEDGED') {
              this.activeIncidentByRuleSensor.set(incKey, alert.id);
            } else {
              this.activeIncidentByRuleSensor.delete(incKey);
            }
            break;
          }
        case 'READING_BATCH':
          {
            const readings = p.readings || [];
            for (const r of readings) {
              this.applyReadingToMemory(r, false);
            }
            break;
          }
      }
    });
  }
  rebuildRuleIndex() {
    this.rulesByMetric.clear();
    for (const rule of this.rules.values()) {
      if (!rule.enabled) continue;
      let list = this.rulesByMetric.get(rule.metricType);
      if (!list) {
        list = [];
        this.rulesByMetric.set(rule.metricType, list);
      }
      list.push(rule);

      // Ensure all streams for this metric pre-register any time-window length
      if (rule.kind === 'TIME_WINDOW' && rule.windowSeconds) {
        for (const stream of this.streams.values()) {
          if (stream.metricType === rule.metricType) {
            stream.registerWindow(rule.windowSeconds);
          }
        }
      }
    }
  }
  getOrCreateStream(sensorId, metricType) {
    const key = `${sensorId}::${metricType}`;
    let stream = this.streams.get(key);
    if (!stream) {
      stream = new _slidingWindow.CircularSensorStream(sensorId, metricType, 3600);
      // Pre-register active time windows for this metric
      const candidateRules = this.rulesByMetric.get(metricType) || [];
      for (const rule of candidateRules) {
        if (rule.kind === 'TIME_WINDOW' && rule.windowSeconds) {
          stream.registerWindow(rule.windowSeconds);
        }
      }
      this.streams.set(key, stream);
    }
    return stream;
  }

  /**
   * Ingests a single reading or a batch of readings.
   * Designed to process 10,000+ readings in well under 100ms of CPU time.
   */
  ingestReadings(readings, isBatchRequest) {
    if (isBatchRequest) {
      this.totalBatchRequests++;
    } else {
      this.totalSingleRequests++;
    }
    const newlyTriggered = [];
    for (let i = 0; i < readings.length; i++) {
      const r = readings[i];
      const alerts = this.applyReadingToMemory(r, true);
      if (alerts.length > 0) {
        for (let j = 0; j < alerts.length; j++) {
          newlyTriggered.push(alerts[j]);
        }
      }
    }

    // Group-commit write-behind to WAL so disk I/O never stalls ingestion bursts
    this.wal.enqueueWriteBehind('READING_BATCH', {
      readings
    }, 2500, 20);
    return {
      ingested: readings.length,
      alertsTriggered: newlyTriggered
    };
  }
  applyReadingToMemory(reading, evaluateRules) {
    const tsMs = Date.parse(reading.timestamp) || Date.now();
    this.totalReadingsIngested++;
    this.ringBuffer.push(reading.sensorId, reading.type, reading.value, tsMs);
    const stream = this.getOrCreateStream(reading.sensorId, reading.type);
    const {
      zScore
    } = stream.record(reading.value, tsMs);
    if (!evaluateRules) {
      return [];
    }
    const candidateRules = this.rulesByMetric.get(reading.type);
    if (!candidateRules || candidateRules.length === 0) {
      return [];
    }
    const triggeredAlerts = [];
    for (let i = 0; i < candidateRules.length; i++) {
      const rule = candidateRules[i];
      if (!rule.enabled) continue;
      if (rule.sensorId !== '*' && rule.sensorId !== reading.sensorId) continue;
      let evaluatedValue = reading.value;
      let sampleCount = 1;
      if (rule.kind === 'TIME_WINDOW') {
        const windowSec = rule.windowSeconds || 600;
        const agg = rule.aggregator || 'AVG';
        const winResult = stream.evaluateWindow(windowSec, agg);
        evaluatedValue = winResult.value;
        sampleCount = winResult.sampleCount;
        const minSamples = rule.minSamples ?? 1;
        if (sampleCount < minSamples) {
          continue;
        }
      }
      if (this.compare(evaluatedValue, rule.operator, rule.threshold)) {
        const alert = this.recordAlertTrigger(rule, reading, evaluatedValue, sampleCount, zScore, tsMs);
        if (alert) {
          triggeredAlerts.push(alert);
        }
      }
    }
    return triggeredAlerts;
  }
  compare(lhs, op, rhs) {
    switch (op) {
      case '>':
        return lhs > rhs;
      case '>=':
        return lhs >= rhs;
      case '<':
        return lhs < rhs;
      case '<=':
        return lhs <= rhs;
      case '==':
        return Math.abs(lhs - rhs) < 1e-9;
      default:
        return false;
    }
  }
  recordAlertTrigger(rule, reading, evaluatedValue, sampleCount, zScore, readingTsMs) {
    const incKey = `${rule.id}::${reading.sensorId}`;
    const nowIso = new Date().toISOString();

    // Check cooldown window if configured
    const cooldownMs = rule.cooldownMs ?? 0;
    if (cooldownMs > 0) {
      const lastMs = this.lastTriggerMsByRuleSensor.get(incKey);
      if (lastMs !== undefined && readingTsMs - lastMs < cooldownMs) {
        // Still update occurrences on active incident if one is open
        const existingId = this.activeIncidentByRuleSensor.get(incKey);
        if (existingId) {
          const existing = this.alerts.get(existingId);
          if (existing && existing.status !== 'RESOLVED') {
            existing.occurrences += 1;
            existing.triggeredValue = Number(evaluatedValue.toFixed(4));
            existing.lastTriggeredAt = nowIso;
            existing.sampleCount = sampleCount;
          }
        }
        return null;
      }
    }
    this.lastTriggerMsByRuleSensor.set(incKey, readingTsMs);

    // If an unresolved incident already exists for this (ruleId, sensorId) and cooldownMs > 0, coalesce into it
    const activeId = this.activeIncidentByRuleSensor.get(incKey);
    if (activeId && cooldownMs > 0) {
      const existing = this.alerts.get(activeId);
      if (existing && existing.status !== 'RESOLVED') {
        existing.occurrences += 1;
        existing.triggeredValue = Number(evaluatedValue.toFixed(4));
        existing.lastTriggeredAt = nowIso;
        existing.sampleCount = sampleCount;
        existing.anomalyZScore = Number(zScore.toFixed(2));
        this.wal.enqueueWriteBehind('ALERT_UPSERT', existing);
        return existing;
      }
    }
    const alert = {
      id: `alt_${crypto.randomUUID().slice(0, 8)}_${++this.totalAlertsTriggered}`,
      ruleId: rule.id,
      ruleName: rule.name,
      ruleKind: rule.kind,
      sensorId: reading.sensorId,
      metricType: reading.type,
      triggeredValue: Number(evaluatedValue.toFixed(4)),
      threshold: rule.threshold,
      operator: rule.operator,
      windowSeconds: rule.windowSeconds,
      aggregator: rule.aggregator,
      sampleCount,
      anomalyZScore: Number(zScore.toFixed(2)),
      status: 'OPEN',
      occurrences: 1,
      firstTriggeredAt: nowIso,
      lastTriggeredAt: nowIso,
      readingTimestamp: reading.timestamp
    };
    this.alerts.set(alert.id, alert);
    this.activeIncidentByRuleSensor.set(incKey, alert.id);
    this.wal.enqueueWriteBehind('ALERT_UPSERT', alert);
    this.emitSse('alert_triggered', alert);
    return alert;
  }

  // ---- Organizer Rule Management CRUD ----

  createRule(input) {
    if (!input.name || !input.metricType || !input.kind || !input.operator || input.threshold === undefined) {
      throw new Error('Missing required rule fields: name, metricType, kind, operator, threshold');
    }
    if (input.kind === 'TIME_WINDOW' && (!input.windowSeconds || input.windowSeconds <= 0)) {
      throw new Error('TIME_WINDOW rules require windowSeconds > 0');
    }
    const now = new Date().toISOString();
    const rule = {
      id: input.id || `rule_${crypto.randomUUID().slice(0, 8)}`,
      name: input.name,
      description: input.description || '',
      sensorId: input.sensorId || '*',
      metricType: input.metricType,
      kind: input.kind,
      operator: input.operator,
      threshold: Number(input.threshold),
      windowSeconds: input.kind === 'TIME_WINDOW' ? Number(input.windowSeconds) : undefined,
      aggregator: input.kind === 'TIME_WINDOW' ? input.aggregator || 'AVG' : undefined,
      minSamples: input.minSamples ?? 1,
      cooldownMs: input.cooldownMs ?? 0,
      enabled: input.enabled !== undefined ? Boolean(input.enabled) : true,
      createdAt: now,
      updatedAt: now
    };
    this.rules.set(rule.id, rule);
    this.rebuildRuleIndex();
    this.wal.appendSyncFsync('RULE_UPSERT', rule);
    this.emitSse('rule_updated', rule);
    return rule;
  }
  updateRule(id, patch) {
    const existing = this.rules.get(id);
    if (!existing) {
      throw new Error(`Rule not found: ${id}`);
    }
    const updated = {
      ...existing,
      ...patch,
      id: existing.id,
      updatedAt: new Date().toISOString()
    };
    if (updated.kind === 'TIME_WINDOW' && (!updated.windowSeconds || updated.windowSeconds <= 0)) {
      throw new Error('TIME_WINDOW rules require windowSeconds > 0');
    }
    this.rules.set(id, updated);
    this.rebuildRuleIndex();
    this.wal.appendSyncFsync('RULE_UPSERT', updated);
    this.emitSse('rule_updated', updated);
    return updated;
  }
  setRuleEnabled(id, enabled) {
    return this.updateRule(id, {
      enabled
    });
  }
  listRules() {
    return Array.from(this.rules.values());
  }
  getRule(id) {
    return this.rules.get(id);
  }

  // ---- Organizer Alert Lifecycle Management ----

  listAlerts(filter) {
    let list = Array.from(this.alerts.values());
    if (filter?.status) {
      list = list.filter(a => a.status === filter.status);
    }
    if (filter?.sensorId) {
      list = list.filter(a => a.sensorId === filter.sensorId);
    }
    if (filter?.ruleId) {
      list = list.filter(a => a.ruleId === filter.ruleId);
    }
    if (filter?.metricType) {
      list = list.filter(a => a.metricType === filter.metricType);
    }
    return list.sort((a, b) => b.lastTriggeredAt.localeCompare(a.lastTriggeredAt));
  }
  acknowledgeAlert(id, organizer = 'campus-ops', note) {
    const alert = this.alerts.get(id);
    if (!alert) {
      throw new Error(`Alert not found: ${id}`);
    }
    if (alert.status === 'RESOLVED') {
      throw new Error(`Cannot acknowledge already RESOLVED alert: ${id}`);
    }
    alert.status = 'ACKNOWLEDGED';
    alert.acknowledgedAt = new Date().toISOString();
    alert.acknowledgedBy = organizer;
    if (note) alert.acknowledgeNote = note;
    this.wal.appendSyncFsync('ALERT_UPSERT', alert);
    this.emitSse('alert_updated', alert);
    return alert;
  }
  resolveAlert(id, organizer = 'campus-ops', resolutionNote) {
    const alert = this.alerts.get(id);
    if (!alert) {
      throw new Error(`Alert not found: ${id}`);
    }
    alert.status = 'RESOLVED';
    alert.resolvedAt = new Date().toISOString();
    alert.resolvedBy = organizer;
    if (resolutionNote) alert.resolutionNote = resolutionNote;
    const incKey = `${alert.ruleId}::${alert.sensorId}`;
    if (this.activeIncidentByRuleSensor.get(incKey) === alert.id) {
      this.activeIncidentByRuleSensor.delete(incKey);
    }
    this.wal.appendSyncFsync('ALERT_UPSERT', alert);
    this.emitSse('alert_updated', alert);
    return alert;
  }
  getStreamSnapshots(windowSeconds = 600) {
    const snapshots = [];
    for (const stream of this.streams.values()) {
      snapshots.push(stream.getSnapshot(windowSeconds));
    }
    return snapshots;
  }
  getMetrics() {
    this.wal.flushWriteBehindSync();
    return {
      totalReadingsIngested: this.totalReadingsIngested,
      totalSingleRequests: this.totalSingleRequests,
      totalBatchRequests: this.totalBatchRequests,
      activeSensorStreams: this.streams.size,
      totalRules: this.rules.size,
      enabledRules: Array.from(this.rules.values()).filter(r => r.enabled).length,
      totalAlerts: this.alerts.size,
      openAlerts: Array.from(this.alerts.values()).filter(a => a.status === 'OPEN').length,
      acknowledgedAlerts: Array.from(this.alerts.values()).filter(a => a.status === 'ACKNOWLEDGED').length,
      resolvedAlerts: Array.from(this.alerts.values()).filter(a => a.status === 'RESOLVED').length,
      ringBuffer: this.ringBuffer.getStats(),
      wal: this.wal.getStats(),
      memoryUsageMb: Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2))
    };
  }
  subscribeSse(listener) {
    this.sseListeners.add(listener);
    return () => this.sseListeners.delete(listener);
  }
  emitSse(event, data) {
    for (const listener of this.sseListeners) {
      try {
        listener(event, data);
      } catch (_) {}
    }
  }
  close() {
    this.wal.close();
  }
}
exports.SensorMonitoringEngine = SensorMonitoringEngine;