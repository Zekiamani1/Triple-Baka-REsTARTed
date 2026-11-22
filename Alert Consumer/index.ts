import { createServer, type Server } from "node:http";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";
import type { Span } from "@opentelemetry/api";
import { kafkaConfig, healthPort } from "./config";
import {
  getDeliveryId,
  isCoastalEvent,
  isCriticalCoastal,
  isEmergency,
  parseHazardMessage,
  PoisonPillError,
} from "./message";
import { deliverWithRetry } from "./delivery";
import {
  extractContextFromKafkaHeaders,
  initTelemetry,
  logError,
  logInfo,
  logWarn,
  recordAlertTriggered,
  recordMessageConsumed,
  recordNotificationDelivery,
  SCOPE_NAME,
  SpanKind,
  SpanStatusCode,
  trace,
} from "./telemetry";

const shutdownTelemetry = initTelemetry();
const kafka = new Kafka({ clientId: kafkaConfig.clientId, brokers: kafkaConfig.brokers });
const generalConsumer = kafka.consumer({ groupId: kafkaConfig.groupId });
const criticalConsumer = kafka.consumer({ groupId: kafkaConfig.criticalGroupId });

let ready = false;
let healthServer: Server | undefined;
let shutdownPromise: Promise<void> | undefined;

function startHealthServer(): Promise<Server> {
  const server = createServer((request, response) => {
    if (request.url === "/health" || request.url === "/healthz") {
      response.writeHead(200, { "content-type": "text/plain" });
      response.end("UP");
      return;
    }
    if (request.url === "/readyz") {
      response.writeHead(ready ? 200 : 503, { "content-type": "text/plain" });
      response.end(ready ? "READY" : "NOT READY");
      return;
    }
    response.writeHead(404);
    response.end();
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(healthPort, () => {
      server.removeListener("error", reject);
      resolve(server);
    });
  });
}

function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
}

async function commitMessage(consumer: Consumer, payload: EachMessagePayload): Promise<void> {
  await consumer.commitOffsets([{
    topic: payload.topic,
    partition: payload.partition,
    offset: nextOffset(payload.message.offset),
  }]);
}

async function withConsumerSpan<T>(
  payload: EachMessagePayload,
  operation: (span: Span) => Promise<T>,
): Promise<T> {
  const parentContext = extractContextFromKafkaHeaders(payload.message.headers);
  return trace.getTracer(SCOPE_NAME).startActiveSpan(
    `kafka.consume ${payload.topic}`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        "messaging.system": "kafka",
        "messaging.operation": "process",
        "messaging.destination.name": payload.topic,
        "messaging.kafka.destination.partition": payload.partition,
        "messaging.kafka.consumer.group": payload.topic === kafkaConfig.criticalTopic
          ? kafkaConfig.criticalGroupId
          : kafkaConfig.groupId,
        "messaging.kafka.message.offset": payload.message.offset,
      },
    },
    parentContext,
    async (span) => {
      try {
        const result = await operation(span);
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (error) {
        span.recordException(error instanceof Error ? error : new Error(String(error)));
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
}

async function handleGeneralMessage(payload: EachMessagePayload): Promise<void> {
  const startedAt = performance.now();
  await withConsumerSpan(payload, async (span) => {
    const event = parseHazardMessage(payload.message.value);
    const source = String(event.source ?? "UNKNOWN");
    const hazardType = String(event.hazard_type ?? "UNKNOWN");
    const severity = String(event.severity ?? "UNKNOWN");

    span.setAttributes({
      "hazard.id": String(event.hazard_id ?? ""),
      "hazard.source": source,
      "hazard.type": hazardType,
      "hazard.severity": severity,
    });

    // Coastal critical events are delivered only by the dedicated hazard-critical worker.
    // Skipping them here prevents duplicate notifications from hazard-events.
    if (isCoastalEvent(event)) {
      logInfo("coastal event handled by dedicated critical route", {
        hazard_id: event.hazard_id,
        source_ref_id: event.source_ref_id,
        critical: isCriticalCoastal(event),
      });
      recordMessageConsumed(
        payload.topic, source, hazardType, severity, isCriticalCoastal(event),
        "routed_to_coastal_worker", performance.now() - startedAt,
      );
      await commitMessage(generalConsumer, payload);
      return;
    }

    const emergency = isEmergency(event);
    span.setAttribute("alert.is_emergency", emergency);
    if (emergency) {
      logWarn("EMERGENCY ALERT", { event });
      span.addEvent("emergency_alert_triggered", {
        "hazard.id": String(event.hazard_id ?? ""),
        "hazard.severity": severity,
        "hazard.area_name": String(event.area_name ?? "UNKNOWN"),
      });
      recordAlertTriggered(source, hazardType, severity, String(event.area_name ?? "UNKNOWN"));
    }

    recordMessageConsumed(
      payload.topic, source, hazardType, severity, emergency,
      "success", performance.now() - startedAt,
    );
    await commitMessage(generalConsumer, payload);
  });
}

async function handleCriticalMessage(payload: EachMessagePayload): Promise<void> {
  const startedAt = performance.now();
  await withConsumerSpan(payload, async (span) => {
    const event = parseHazardMessage(payload.message.value);
    const source = String(event.source ?? "UNKNOWN");
    const hazardType = String(event.hazard_type ?? "UNKNOWN");
    const severity = String(event.severity ?? "UNKNOWN");

    span.setAttributes({
      "hazard.id": String(event.hazard_id ?? ""),
      "hazard.source": source,
      "hazard.type": hazardType,
      "hazard.severity": severity,
      "alert.is_critical": isCriticalCoastal(event),
    });

    if (!isCriticalCoastal(event)) {
      // Do not silently acknowledge a malformed or incorrectly routed critical record.
      throw new Error("hazard-critical topic contains an event that is not SENSOR_PANTAI/COASTAL critical=true");
    }

    const deliveryId = getDeliveryId(event);
    const deliveryStartedAt = performance.now();
    try {
      const acknowledgment = await deliverWithRetry(
        event,
        deliveryId,
        () => payload.heartbeat(),
      );
      recordNotificationDelivery("success", performance.now() - deliveryStartedAt);
      logInfo("critical coastal notification acknowledged by Field Receiver", {
        delivery_id: deliveryId,
        receipt_id: acknowledgment.receipt_id,
        received_at: acknowledgment.received_at,
        duplicate: acknowledgment.duplicate,
      });
    } catch (error) {
      recordNotificationDelivery("error", performance.now() - deliveryStartedAt);
      logError("critical coastal notification delivery failed; Kafka offset will not be committed", {
        delivery_id: deliveryId,
        err: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }

    // The receiver returns success only after its durable receipt is stored.
    // Commit the Kafka offset only after that acknowledgment. Record the event
    // as delivered only after the commit succeeds, avoiding duplicate counts
    // when a commit fails and Kafka redelivers the same message.
    await commitMessage(criticalConsumer, payload);
    recordAlertTriggered(source, hazardType, severity, String(event.area_name ?? "UNKNOWN"));
    recordMessageConsumed(
      payload.topic, source, hazardType, severity, true,
      "delivered", performance.now() - startedAt,
    );
  });
}

async function handleWithPoisonPillPolicy(
  consumer: Consumer,
  payload: EachMessagePayload,
  handler: (payload: EachMessagePayload) => Promise<void>,
): Promise<void> {
  const startedAt = performance.now();
  try {
    await handler(payload);
  } catch (error) {
    if (!(error instanceof PoisonPillError) || payload.topic === kafkaConfig.criticalTopic) {
      throw error;
    }
    logError("invalid hazard message skipped by general consumer", {
      topic: payload.topic,
      partition: payload.partition,
      offset: payload.message.offset,
      err: error.message,
    });
    recordMessageConsumed(
      payload.topic, "UNKNOWN", "UNKNOWN", "UNKNOWN", false,
      "poison_pill", performance.now() - startedAt,
    );
    await commitMessage(consumer, payload);
  }
}

async function main(): Promise<void> {
  healthServer = await startHealthServer();
  await Promise.all([generalConsumer.connect(), criticalConsumer.connect()]);
  await Promise.all([
    generalConsumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: kafkaConfig.fromBeginning }),
    criticalConsumer.subscribe({ topic: kafkaConfig.criticalTopic, fromBeginning: kafkaConfig.fromBeginning }),
  ]);

  ready = true;
  logInfo("Alert Consumer started", {
    general_topic: kafkaConfig.topic,
    general_group: kafkaConfig.groupId,
    critical_topic: kafkaConfig.criticalTopic,
    critical_group: kafkaConfig.criticalGroupId,
  });

  await Promise.all([
    generalConsumer.run({
      autoCommit: false,
      eachMessage: (payload) => handleWithPoisonPillPolicy(generalConsumer, payload, handleGeneralMessage),
    }),
    criticalConsumer.run({
      autoCommit: false,
      eachMessage: (payload) => handleWithPoisonPillPolicy(criticalConsumer, payload, handleCriticalMessage),
    }),
  ]);
}

async function shutdown(exitCode = 0): Promise<void> {
  if (shutdownPromise) return shutdownPromise;
  shutdownPromise = (async () => {
    ready = false;
    logInfo("Alert Consumer shutting down");
    const disconnectResults = await Promise.allSettled([
      generalConsumer.disconnect(),
      criticalConsumer.disconnect(),
    ]);
    for (const result of disconnectResults) {
      if (result.status === "rejected") {
        logError("Kafka consumer disconnect failed", {
          err: result.reason instanceof Error ? result.reason.message : String(result.reason),
        });
      }
    }

    if (healthServer) {
      await new Promise<void>((resolve) => healthServer!.close(() => resolve()));
      healthServer = undefined;
    }
    await shutdownTelemetry();
    process.exitCode = exitCode;
  })();
  return shutdownPromise;
}

process.once("SIGINT", () => void shutdown(0));
process.once("SIGTERM", () => void shutdown(0));

main().catch(async (error) => {
  logError("Alert Consumer fatal error", {
    err: error instanceof Error ? error.message : String(error),
  });
  await shutdown(1);
});
