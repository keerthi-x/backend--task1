/**
 * Task 1: Campus Sensor Monitoring and Alerts — Domain Types
 */

export type MetricType = 'temperature' | 'air_quality' | 'occupancy' | 'co2_ppm' | 'humidity' | string;

export interface SensorReading {
  sensorId: string;
  type: MetricType;
  value: number;
  timestamp: string; // ISO-8601 e.g. "2026-10-12T10:30:00Z"
}

export type ComparisonOperator = '>' | '>=' | '<' | '<=' | '==';

export type WindowAggregator = 'AVG' | 'MIN' | 'MAX' | 'COUNT' | 'RATE_OF_CHANGE';

export type RuleKind = 'SINGLE_READING' | 'TIME_WINDOW';

export interface AlertRule {
  id: string;
  name: string;
  description?: string;
  sensorId: string; // specific sensorId like "library-floor-2" or "*" for any sensor of that metric type
  metricType: MetricType;
  kind: RuleKind;
  operator: ComparisonOperator;
  threshold: number;
  /** Required when kind === 'TIME_WINDOW' (e.g., 600 for 10 minutes) */
  windowSeconds?: number;
  /** Defaults to 'AVG' when kind === 'TIME_WINDOW' */
  aggregator?: WindowAggregator;
  /** Minimum readings in window required before evaluating a TIME_WINDOW rule (default: 1) */
  minSamples?: number;
  /** Cooldown/deduplication window in ms to prevent alert storms during 10k/s bursts (0 = fire on every reading) */
  cooldownMs?: number;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
}

export type AlertStatus = 'OPEN' | 'ACKNOWLEDGED' | 'RESOLVED';

export interface AlertRecord {
  id: string;
  ruleId: string;
  ruleName: string;
  ruleKind: RuleKind;
  sensorId: string;
  metricType: MetricType;
  triggeredValue: number;
  threshold: number;
  operator: ComparisonOperator;
  windowSeconds?: number;
  aggregator?: WindowAggregator;
  sampleCount: number;
  /** Statistical Z-score computed via Welford's online algorithm at trigger time */
  anomalyZScore: number;
  status: AlertStatus;
  occurrences: number;
  firstTriggeredAt: string;
  lastTriggeredAt: string;
  readingTimestamp: string;
  acknowledgedAt?: string;
  acknowledgedBy?: string;
  acknowledgeNote?: string;
  resolvedAt?: string;
  resolvedBy?: string;
  resolutionNote?: string;
}

export interface StreamWindowSnapshot {
  sensorId: string;
  metricType: MetricType;
  windowSeconds: number;
  count: number;
  avg: number;
  min: number;
  max: number;
  rateOfChangePerMin: number;
  welfordMean: number;
  welfordStdDev: number;
  latestValue: number;
  latestTimestampMs: number;
}
