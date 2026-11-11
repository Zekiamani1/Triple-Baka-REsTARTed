import {
  extractContextFromKafkaHeaders,
  logError,
  logInfo,
  logWarn,
  recordAlertTriggered,
  recordMessageConsumed,
  SCOPE_NAME,
  shutdownTelemetry,
  SpanKind,
  SpanStatusCode,
  trace,
} from "./telemetry";
import { Kafka } from "kafkajs";
import { createServer } from "node:http";
import { kafkaConfig } from "./config";
import { isEmergency, parseHazardMessage } from "./message";

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
});

const consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

function startHealthServer(): void {
  const port = Number(process.env.HEALTH_PORT ?? "8088");
  createServer((request, response) => {
    if (request.url === "/health") {
      response.writeHead(200);
      response.end("UP");
      return;
    }
    response.writeHead(404);
    response.end();
  }).listen(port);
}

async function main(): Promise<void> {
  startHealthServer();
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: true });
  logInfo("listening for emergency hazard events", { topic: kafkaConfig.topic });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const start = performance.now();
      const parentCtx = extractContextFromKafkaHeaders(message.headers);

      await trace.getTracer(SCOPE_NAME).startActiveSpan(
        `kafka.consume ${topic}`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            "messaging.system": "kafka",
            "messaging.operation": "process",
            "messaging.destination.name": topic,
            "messaging.kafka.destination.partition": partition,
            "messaging.kafka.consumer.group": kafkaConfig.groupId,
            "messaging.kafka.message.offset": message.offset,
          },
        },
        parentCtx,
        async (span) => {
          try {
            const event = parseHazardMessage(message.value);
            const source = String(event.source ?? "UNKNOWN");
            const hazardType = String(event.hazard_type ?? "UNKNOWN");
            const severity = String(event.severity ?? "UNKNOWN");
            const emergency = isEmergency(event);

            span.setAttributes({
              "hazard.id": String(event.hazard_id ?? ""),
              "hazard.source": source,
              "hazard.type": hazardType,
              "hazard.severity": severity,
              "alert.is_emergency": emergency,
            });

            if (emergency) {
              logWarn("EMERGENCY ALERT", { event });
              span.addEvent("emergency_alert_triggered", {
                "hazard.id": String(event.hazard_id ?? ""),
                "hazard.severity": severity,
                "hazard.area_name": String(event.area_name ?? ""),
              });
              recordAlertTriggered(
                source,
                hazardType,
                severity,
                String(event.area_name ?? "UNKNOWN"),
              );
            }

            const durationMs = performance.now() - start;
            recordMessageConsumed(
              topic,
              source,
              hazardType,
              severity,
              emergency,
              "success",
              durationMs,
            );
            span.setStatus({ code: SpanStatusCode.OK });
          } catch (error: any) {
            const durationMs = performance.now() - start;
            recordMessageConsumed(
              topic,
              "UNKNOWN",
              "UNKNOWN",
              "UNKNOWN",
              false,
              "error",
              durationMs,
            );
            logError("failed to process hazard event in alert consumer", {
              err: error instanceof Error ? error.message : String(error),
            });
            span.recordException(error);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: error instanceof Error ? error.message : String(error),
            });
            throw error;
          } finally {
            span.end();
          }
        },
      );
    },
  });
}

async function shutdown(): Promise<void> {
  await consumer.disconnect();
  await shutdownTelemetry();
  process.exit(0);
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
main().catch(async (err) => {
  logError("Alert Consumer fatal error", {
    err: err instanceof Error ? err.message : String(err),
  });
  await shutdownTelemetry();
  process.exit(1);
});
