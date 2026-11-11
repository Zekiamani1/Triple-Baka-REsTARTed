import {
  Kafka,
  Partitioners,
  type Consumer,
  type EachMessagePayload,
  type Message,
  type Producer,
} from "kafkajs";
import { kafka as kafkaConfig } from "./config";
import {
  extractContextFromKafkaHeaders,
  injectContextIntoKafkaHeaders,
  logInfo,
  recordKafkaConsumed,
  recordKafkaPublish,
  SCOPE_NAME,
  SpanKind,
  SpanStatusCode,
  trace,
} from "./telemetry";

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
});

const producer: Producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});
const consumer: Consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

export async function connectKafka(): Promise<void> {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.inTopic, fromBeginning: true });
  logInfo("Kafka producer connected", { brokers: kafkaConfig.brokers });
  logInfo("Kafka consumer listening", { topic: kafkaConfig.inTopic });
}

export async function runIngestConsumer(
  handler: (hazard: Record<string, unknown>) => Promise<void>,
): Promise<void> {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
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
            if (!message.value) {
              throw new Error("Received an empty inbound HazardEvent");
            }

            const hazard = JSON.parse(message.value.toString("utf8"));
            if (typeof hazard !== "object" || hazard === null || Array.isArray(hazard)) {
              throw new Error("Inbound HazardEvent must be a JSON object");
            }

            span.setAttributes({
              "hazard.id": String(hazard.hazard_id ?? ""),
              "hazard.source": String(hazard.source ?? ""),
              "hazard.type": String(hazard.hazard_type ?? ""),
              "hazard.severity": String(hazard.severity ?? ""),
            });

            await handler(hazard as Record<string, unknown>);

            recordKafkaConsumed(topic, "success");
            span.setStatus({ code: SpanStatusCode.OK });
          } catch (err: any) {
            recordKafkaConsumed(topic, "error");
            span.recordException(err);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: err instanceof Error ? err.message : String(err),
            });
            throw err;
          } finally {
            span.end();
          }
        },
      );
    },
  });
}

export function createHazardKafkaMessage(
  hazard: Record<string, unknown>,
): Message {
  if (typeof hazard.hazard_id !== "string" || hazard.hazard_id.trim() === "") {
    throw new Error("Hazard event requires a non-empty hazard_id");
  }

  return {
    key: hazard.hazard_id,
    value: JSON.stringify(hazard),
  };
}

export async function publishHazard(
  hazard: Record<string, unknown>,
): Promise<void> {
  const start = performance.now();
  await trace.getTracer(SCOPE_NAME).startActiveSpan(
    `kafka.publish ${kafkaConfig.outTopic}`,
    {
      kind: SpanKind.PRODUCER,
      attributes: {
        "messaging.system": "kafka",
        "messaging.operation": "publish",
        "messaging.destination.name": kafkaConfig.outTopic,
        "hazard.id": String(hazard.hazard_id ?? ""),
        "hazard.source": String(hazard.source ?? ""),
        "hazard.type": String(hazard.hazard_type ?? ""),
        "hazard.severity": String(hazard.severity ?? ""),
      },
    },
    async (span) => {
      try {
        const baseMessage = createHazardKafkaMessage(hazard);
        const messageWithHeaders: Message = {
          ...baseMessage,
          headers: injectContextIntoKafkaHeaders(baseMessage.headers),
        };

        await producer.send({
          topic: kafkaConfig.outTopic,
          messages: [messageWithHeaders],
        });

        const durationMs = performance.now() - start;
        recordKafkaPublish(
          kafkaConfig.outTopic,
          String(hazard.source ?? "UNKNOWN"),
          String(hazard.severity ?? "UNKNOWN"),
          "success",
          durationMs,
        );
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err: any) {
        const durationMs = performance.now() - start;
        recordKafkaPublish(
          kafkaConfig.outTopic,
          String(hazard.source ?? "UNKNOWN"),
          String(hazard.severity ?? "UNKNOWN"),
          "error",
          durationMs,
        );
        span.recordException(err);
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: err instanceof Error ? err.message : String(err),
        });
        throw err;
      } finally {
        span.end();
      }
    },
  );
}

export async function disconnectKafka(): Promise<void> {
  await consumer.disconnect();
  await producer.disconnect();
}
