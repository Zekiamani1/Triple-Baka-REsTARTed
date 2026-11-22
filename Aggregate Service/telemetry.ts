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
import { GrpcInstrumentation } from "@opentelemetry/instrumentation-grpc";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { MongoDBInstrumentation } from "@opentelemetry/instrumentation-mongodb";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import type { IHeaders } from "kafkajs";

export const SCOPE_NAME = process.env.OTEL_SERVICE_NAME ?? "aggregate-service";

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
    instrumentations: [
      new GrpcInstrumentation(),
      new MongoDBInstrumentation(),
      new HttpInstrumentation(),
    ],
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

export function recordGrpcRequest(method: string, status: string, durationMs: number): void {
  const m = metrics.getMeter(SCOPE_NAME);
  m.createCounter("aggregate.grpc.requests_total", {
    description: "Total number of gRPC requests received by Aggregate Service",
  }).add(1, { method, status });
  m.createHistogram("aggregate.grpc.duration_ms", {
    description: "Duration of gRPC requests in milliseconds",
    unit: "ms",
  }).record(durationMs, { method, status });
}

export function recordIngestEvent(
  source: string,
  hazardType: string,
  severity: string,
  status: string,
  durationMs: number,
): void {
  const m = metrics.getMeter(SCOPE_NAME);
  m.createCounter("aggregate.ingest.events_total", {
    description: "Total number of hazard events processed during ingestion",
  }).add(1, { source, hazard_type: hazardType, severity, status });
  m.createHistogram("aggregate.ingest.duration_ms", {
    description: "Duration of hazard event ingestion in milliseconds",
    unit: "ms",
  }).record(durationMs, { source, status });
}

export function recordKafkaConsumed(topic: string, status: string): void {
  metrics
    .getMeter(SCOPE_NAME)
    .createCounter("aggregate.kafka.consumed_total", {
      description: "Total number of messages consumed from Kafka",
    })
    .add(1, { topic, status });
}

export function recordKafkaPublish(
  topic: string,
  source: string,
  severity: string,
  status: string,
  durationMs: number,
): void {
  const m = metrics.getMeter(SCOPE_NAME);
  m.createCounter("aggregate.kafka.published_total", {
    description: "Total number of hazard events published to Kafka",
  }).add(1, { topic, source, severity, status });
  m.createHistogram("aggregate.kafka.publish_duration_ms", {
    description: "Duration of publishing hazard events to Kafka in milliseconds",
    unit: "ms",
  }).record(durationMs, { topic, status });
}

export function recordMongoOp(operation: string, status: string, durationMs: number): void {
  metrics
    .getMeter(SCOPE_NAME)
    .createHistogram("aggregate.mongo.operation_duration_ms", {
      description: "Duration of MongoDB operations in milliseconds",
      unit: "ms",
    })
    .record(durationMs, { operation, status });
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

export function injectContextIntoKafkaHeaders(headers: IHeaders = {}): IHeaders {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  const result: IHeaders = { ...headers };
  for (const [k, v] of Object.entries(carrier)) {
    result[k] = v;
  }
  return result;
}

export { SpanKind, SpanStatusCode, trace };
