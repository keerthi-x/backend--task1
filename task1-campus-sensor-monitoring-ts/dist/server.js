"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.startTask1Server = startTask1Server;
var http = _interopRequireWildcard(require("http"));
var path = _interopRequireWildcard(require("path"));
var _engine = require("./engine");
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function (nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || typeof obj !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        resolve(raw ? JSON.parse(raw) : {});
      } catch (err) {
        reject(new Error('Invalid JSON payload'));
      }
    });
    req.on('error', reject);
  });
}
function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key'
  });
  res.end(body);
}
function startTask1Server(port = 4001, walPath) {
  const defaultWal = walPath || path.resolve(__dirname, '../data/task1-sensor-telemetry.wal.ndjson');
  const engine = new _engine.SensorMonitoringEngine(defaultWal);
  const server = http.createServer(async (req, res) => {
    try {
      const method = req.method || 'GET';
      const parsedUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
      const pathname = parsedUrl.pathname;
      if (method === 'OPTIONS') {
        res.writeHead(204, {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key'
        });
        res.end();
        return;
      }

      // 1. Health & Live Metrics
      if (method === 'GET' && pathname === '/api/health') {
        sendJson(res, 200, {
          status: 'ok',
          service: 'task1-sensor-monitoring',
          metrics: engine.getMetrics()
        });
        return;
      }
      if (method === 'GET' && pathname === '/api/streams') {
        const winSec = Number(parsedUrl.searchParams.get('windowSeconds') || 600);
        sendJson(res, 200, {
          streams: engine.getStreamSnapshots(winSec)
        });
        return;
      }

      // 2. Server-Sent Events (SSE) Real-Time Stream for Organizers
      if (method === 'GET' && pathname === '/api/events') {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
          'Access-Control-Allow-Origin': '*'
        });
        res.write(`event: connected\ndata: ${JSON.stringify(engine.getMetrics())}\n\n`);
        const unsubscribe = engine.subscribeSse((event, data) => {
          res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
        });
        req.on('close', unsubscribe);
        return;
      }

      // 3. Ingest Readings (Single or Batch)
      if (method === 'POST' && (pathname === '/api/readings' || pathname === '/api/readings/batch')) {
        const body = await readJsonBody(req);
        let readings;
        let isBatch = pathname === '/api/readings/batch';
        if (Array.isArray(body)) {
          readings = body;
          isBatch = true;
        } else if (body && Array.isArray(body.readings)) {
          readings = body.readings;
          isBatch = true;
        } else if (body && typeof body.sensorId === 'string') {
          readings = [body];
        } else {
          sendJson(res, 400, {
            error: 'Payload must be a single reading { sensorId, type, value, timestamp } or { readings: [...] }'
          });
          return;
        }

        // Fast validation pass
        for (let i = 0; i < readings.length; i++) {
          const r = readings[i];
          if (!r || typeof r.sensorId !== 'string' || typeof r.type !== 'string' || typeof r.value !== 'number') {
            sendJson(res, 400, {
              error: `Invalid reading at index ${i}`
            });
            return;
          }
          if (!r.timestamp) {
            r.timestamp = new Date().toISOString();
          }
        }
        const result = engine.ingestReadings(readings, isBatch);
        sendJson(res, 201, {
          ingested: result.ingested,
          alertsTriggeredCount: result.alertsTriggered.length,
          alertsTriggered: result.alertsTriggered
        });
        return;
      }

      // 4. Alert Rules CRUD & Enable/Disable
      if (method === 'GET' && pathname === '/api/rules') {
        sendJson(res, 200, {
          rules: engine.listRules()
        });
        return;
      }
      if (method === 'POST' && pathname === '/api/rules') {
        const body = await readJsonBody(req);
        const created = engine.createRule(body);
        sendJson(res, 201, {
          rule: created
        });
        return;
      }
      const ruleMatch = pathname.match(/^\/api\/rules\/([^/]+)(?:\/(enable|disable))?$/);
      if (ruleMatch) {
        const ruleId = decodeURIComponent(ruleMatch[1]);
        const action = ruleMatch[2];
        if (method === 'POST' && action === 'enable') {
          const rule = engine.setRuleEnabled(ruleId, true);
          sendJson(res, 200, {
            rule
          });
          return;
        }
        if (method === 'POST' && action === 'disable') {
          const rule = engine.setRuleEnabled(ruleId, false);
          sendJson(res, 200, {
            rule
          });
          return;
        }
        if ((method === 'PUT' || method === 'PATCH') && !action) {
          const body = await readJsonBody(req);
          const rule = engine.updateRule(ruleId, body);
          sendJson(res, 200, {
            rule
          });
          return;
        }
      }

      // 5. Alerts View, Acknowledge, and Resolve
      if (method === 'GET' && pathname === '/api/alerts') {
        const status = parsedUrl.searchParams.get('status') || undefined;
        const sensorId = parsedUrl.searchParams.get('sensorId') || undefined;
        const ruleId = parsedUrl.searchParams.get('ruleId') || undefined;
        const metricType = parsedUrl.searchParams.get('type') || undefined;
        sendJson(res, 200, {
          alerts: engine.listAlerts({
            status,
            sensorId,
            ruleId,
            metricType
          })
        });
        return;
      }
      const alertMatch = pathname.match(/^\/api\/alerts\/([^/]+)\/(acknowledge|resolve)$/);
      if (method === 'POST' && alertMatch) {
        const alertId = decodeURIComponent(alertMatch[1]);
        const action = alertMatch[2];
        const body = await readJsonBody(req);
        if (action === 'acknowledge') {
          const updated = engine.acknowledgeAlert(alertId, body.organizer, body.note);
          sendJson(res, 200, {
            alert: updated
          });
          return;
        }
        if (action === 'resolve') {
          const updated = engine.resolveAlert(alertId, body.organizer, body.resolutionNote);
          sendJson(res, 200, {
            alert: updated
          });
          return;
        }
      }
      sendJson(res, 404, {
        error: `Route not found: ${method} ${pathname}`
      });
    } catch (err) {
      const msg = err?.message || 'Internal server error';
      const status = msg.includes('not found') ? 404 : 400;
      sendJson(res, status, {
        error: msg
      });
    }
  });
  return new Promise(resolve => {
    server.listen(port, () => {
      const addr = server.address();
      const actualPort = typeof addr === 'object' && addr ? addr.port : port;
      resolve({
        server,
        engine,
        port: actualPort,
        close: () => new Promise(resClose => {
          engine.close();
          server.close(() => resClose());
        })
      });
    });
  });
}
if (require.main === module) {
  const port = Number(process.env.PORT || 4001);
  startTask1Server(port).then(handle => {
    console.log(`[Task 1 Server] Campus Sensor Monitoring & Alerts listening on http://localhost:${handle.port}`);
  });
}