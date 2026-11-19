import {
  context,
  metrics,
  propagation,
  trace,
  SpanKind,
  SpanStatusCode,
  type Context,
} from "@opentelemetry/api";
import { logs, SeverityNumber, type AnyValueMap } from "@opentelemetry/api-logs";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-grpc";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-grpc";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import type { IHeaders } from "kafkajs";

export const SCOPE_NAME = process.env.OTEL_SERVICE_NAME ?? "alert-consumer";

export function initTelemetry(): () => Promise<void> {
  if (process.env.OTEL_SDK_DISABLED === "true") {
    return async () => {};
  }

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: SCOPE_NAME,
      [ATTR_SERVICE_VERSION]: "1.0.0",
    }),
    traceExporter: new OTLPTraceExporter(),
    metricReader: new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter(),
      exportIntervalMillis: Number(process.env.OTEL_METRIC_EXPORT_INTERVAL ?? "10000"),
    }),
    logRecordProcessors: [
      new BatchLogRecordProcessor({ exporter: new OTLPLogExporter() }),
    ],
    instrumentations: [new HttpInstrumentation()],
  });

  sdk.start();

  return async () => {
    await sdk.shutdown().catch((err) => {
      logError("failed to shutdown OpenTelemetry SDK", { err: String(err) });
    });
  };
}

const SEVERITY_MAP: Record<"INFO" | "WARN" | "ERROR", SeverityNumber> = {
  INFO: SeverityNumber.INFO,
  WARN: SeverityNumber.WARN,
  ERROR: SeverityNumber.ERROR,
};

function writeLog(level: "INFO" | "WARN" | "ERROR", msg: string, attrs: Record<string, unknown> = {}): void {
  const activeCtx = context.active();
  const spanCtx = trace.getSpan(activeCtx)?.spanContext();
  logs.getLogger(SCOPE_NAME).emit({
    severityNumber: SEVERITY_MAP[level],
    severityText: level,
    body: msg,
    attributes: attrs as AnyValueMap,
    context: activeCtx,
  });
  const record: Record<string, unknown> = {
    time: new Date().toISOString(),
    level,
    msg,
    ...(spanCtx
      ? {
          trace_id: spanCtx.traceId,
          span_id: spanCtx.spanId,
        }
      : {}),
    ...attrs,
  };
  const line = JSON.stringify(record) + "\n";
  if (level === "ERROR") {
    process.stderr.write(line);
  } else {
    process.stdout.write(line);
  }
}

export function logInfo(msg: string, attrs?: Record<string, unknown>): void {
  writeLog("INFO", msg, attrs);
}

export function logWarn(msg: string, attrs?: Record<string, unknown>): void {
  writeLog("WARN", msg, attrs);
}

export function logError(msg: string, attrs?: Record<string, unknown>): void {
  writeLog("ERROR", msg, attrs);
}

export function recordAlertTriggered(
  source: string,
  hazardType: string,
  severity: string,
  areaName: string,
): void {
  metrics
    .getMeter(SCOPE_NAME)
    .createCounter("alert_consumer.alerts.triggered_total", {
      description: "Total number of emergency alerts triggered (SIAGA/AWAS)",
    })
    .add(1, { source, hazard_type: hazardType, severity, area_name: areaName });
}

export function recordMessageConsumed(
  topic: string,
  source: string,
  hazardType: string,
  severity: string,
  isEmergency: boolean,
  status: string,
  durationMs: number,
): void {
  const m = metrics.getMeter(SCOPE_NAME);
  m.createCounter("alert_consumer.messages.consumed_total", {
    description: "Total number of hazard events consumed by Alert Consumer",
  }).add(1, {
    topic,
    source,
    hazard_type: hazardType,
    severity,
    is_emergency: String(isEmergency),
    status,
  });
  m.createHistogram("alert_consumer.messages.processing_duration_ms", {
    description: "Duration of processing a Kafka hazard event in milliseconds",
    unit: "ms",
  }).record(durationMs, { topic, status });
}

export function extractContextFromKafkaHeaders(headers?: IHeaders): Context {
  const carrier: Record<string, string> = {};
  if (headers) {
    for (const [k, v] of Object.entries(headers)) {
      if (Buffer.isBuffer(v)) {
        carrier[k] = v.toString("utf8");
      } else if (typeof v === "string") {
        carrier[k] = v;
      } else if (Array.isArray(v) && v.length > 0) {
        const first = v[0];
        carrier[k] = Buffer.isBuffer(first) ? first.toString("utf8") : String(first);
      }
    }
  }
  return propagation.extract(context.active(), carrier);
}

export { SpanKind, SpanStatusCode, trace };
