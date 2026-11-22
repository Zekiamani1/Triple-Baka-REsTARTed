import {
  Kafka,
  Partitioners,
  type Consumer,
  type EachMessagePayload,
  type Message,
  type Producer,
} from "kafkajs";
import { kafka as kafkaConfig } from "./config";
import { PoisonPillError, validateAndDecodeJsonPayload } from "./poisonGuard";
import {
  extractContextFromKafkaHeaders,
  injectContextIntoKafkaHeaders,
  logInfo,
  logWarn,
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
let consumerReady = false;
let producerConnected = false;
let consumerConnected = false;
let producerConnecting: Promise<void> | undefined;

consumer.on(consumer.events.GROUP_JOIN, () => { consumerReady = true; });
consumer.on(consumer.events.CRASH, () => { consumerReady = false; });
consumer.on(consumer.events.STOP, () => { consumerReady = false; });
consumer.on(consumer.events.DISCONNECT, () => { consumerReady = false; });

export function isKafkaReady(): boolean {
  return consumerReady;
}

export async function ensureProducerConnected(): Promise<void> {
  if (producerConnected) return;
  if (!producerConnecting) {
    producerConnecting = producer.connect().then(() => {
      producerConnected = true;
    }).finally(() => {
      producerConnecting = undefined;
    });
  }
  await producerConnecting;
}

export async function stopIngestConsumer(): Promise<void> {
  consumerReady = false;
  if (consumerConnected) await consumer.stop();
}

export async function connectKafka(): Promise<void> {
  await ensureProducerConnected();
  if (!consumerConnected) {
    await consumer.connect();
    consumerConnected = true;
  }
  await consumer.subscribe({ topic: kafkaConfig.inTopic, fromBeginning: true });
  const topics = [kafkaConfig.inTopic];
  if (kafkaConfig.subscribeAuxiliaryTopics) {
    await consumer.subscribe({ topic: kafkaConfig.fieldReportsTopic, fromBeginning: true });
    await consumer.subscribe({ topic: kafkaConfig.dlqTopic, fromBeginning: true });
    await consumer.subscribe({ topic: kafkaConfig.retryTopic, fromBeginning: true });
    topics.push(kafkaConfig.fieldReportsTopic, kafkaConfig.dlqTopic, kafkaConfig.retryTopic);
  }
  logInfo("Kafka producer connected", { brokers: kafkaConfig.brokers });
  logInfo("Kafka consumer listening", { topics });
}

export interface IngestConsumerHandlers {
  onHazard: (hazard: Record<string, unknown>) => Promise<void>;
  onDlqMessage?: (dlq: Record<string, unknown>) => Promise<void>;
  onPoisonPill?: (info: {
    topic: string;
    rawPayload: string;
    rawEncoding: "utf8" | "base64";
    reason: string;
    source: string;
  }) => Promise<void>;
  onRetryableFailure?: (info: {
    messageId?: string;
    source: string;
    sourceRefId: string;
    rawPayload: string;
    reason: string;
    attempts: number;
    exhausted: boolean;
  }) => Promise<string>;
  onRetryRecovered?: (messageId: string) => Promise<void>;
}

function decodeRawForDlq(value: Buffer | null): { rawPayload: string; rawEncoding: "utf8" | "base64" } {
  if (!value) return { rawPayload: "", rawEncoding: "utf8" };
  try {
    return { rawPayload: new TextDecoder("utf-8", { fatal: true }).decode(value), rawEncoding: "utf8" };
  } catch {
    return { rawPayload: value.toString("base64"), rawEncoding: "base64" };
  }
}

export async function runIngestConsumer(
  handlerOrOpts: ((hazard: Record<string, unknown>) => Promise<void>) | IngestConsumerHandlers,
): Promise<void> {
  const handlers: IngestConsumerHandlers =
    typeof handlerOrOpts === "function" ? { onHazard: handlerOrOpts } : handlerOrOpts;

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
            let payload: Record<string, unknown>;
            try {
              payload = validateAndDecodeJsonPayload(message.value);
            } catch (err) {
              if (err instanceof PoisonPillError) {
                recordKafkaConsumed(topic, "poison_pill");
                logWarn("poison pill message parked and offset committed", {
                  topic,
                  partition,
                  offset: message.offset,
                  reason: err.reason,
                });
                if (handlers.onPoisonPill) {
                  const raw = decodeRawForDlq(message.value);
                  await handlers.onPoisonPill({
                    topic,
                    rawPayload: raw.rawPayload,
                    rawEncoding: raw.rawEncoding,
                    reason: err.reason,
                    source: topic === kafkaConfig.fieldReportsTopic ? "TIM_LAPANGAN" : "UNKNOWN",
                  });
                }
                span.setStatus({ code: SpanStatusCode.ERROR, message: err.reason });
                return; // Commit offset without crashing
              }
              throw err;
            }

            if (topic === kafkaConfig.dlqTopic) {
              if (handlers.onDlqMessage) {
                await handlers.onDlqMessage(payload);
              }
              recordKafkaConsumed(topic, "success");
              span.setStatus({ code: SpanStatusCode.OK });
              return;
            }

            const isRetryTopic = topic === kafkaConfig.retryTopic;
            const retryMeta = isRetryTopic ? (payload as { _retry_attempts?: number; _dlq_message_id?: string; hazard?: Record<string, unknown> }) : null;
            const hazard = isRetryTopic && retryMeta?.hazard ? retryMeta.hazard : payload;
            const attempts = isRetryTopic ? Number(retryMeta?._retry_attempts ?? 1) : 0;

            if (isRetryTopic && attempts > 0) {
              const backoffMs = Math.min(500 * 2 ** (attempts - 1), 10000);
              await new Promise((r) => setTimeout(r, backoffMs));
            }
            if (kafkaConfig.expectedSource && hazard.source !== kafkaConfig.expectedSource) {
              throw new Error(`Expected ${kafkaConfig.expectedSource} on ${topic}`);
            }

            span.setAttributes({
              "hazard.id": String(hazard.hazard_id ?? ""),
              "hazard.source": String(hazard.source ?? ""),
              "hazard.type": String(hazard.hazard_type ?? ""),
              "hazard.severity": String(hazard.severity ?? ""),
            });

            try {
              await handlers.onHazard(hazard);
              if (isRetryTopic && retryMeta?._dlq_message_id && handlers.onRetryRecovered) {
                await handlers.onRetryRecovered(retryMeta._dlq_message_id);
              }
              recordKafkaConsumed(topic, "success");
              span.setStatus({ code: SpanStatusCode.OK });
            } catch (err: any) {
              const nextAttempts = attempts + 1;
              const maxAttempts = 5;
              const exhausted = nextAttempts >= maxAttempts;
              const reason = err instanceof Error ? err.message : String(err);

              if (handlers.onRetryableFailure) {
                const msgId = await handlers.onRetryableFailure({
                  messageId: retryMeta?._dlq_message_id,
                  source: String(hazard.source ?? "UNKNOWN"),
                  sourceRefId: String(hazard.source_ref_id ?? ""),
                  rawPayload: JSON.stringify(hazard),
                  reason,
                  attempts: nextAttempts,
                  exhausted,
                });
                if (!exhausted) {
                  await producer.send({
                    topic: kafkaConfig.retryTopic,
                    messages: [
                      {
                        key: String(hazard.source_ref_id ?? hazard.hazard_id ?? msgId),
                        value: JSON.stringify({
                          _dlq_message_id: msgId,
                          _retry_attempts: nextAttempts,
                          hazard,
                        }),
                      },
                    ],
                  });
                }
                recordKafkaConsumed(topic, "retry");
                span.setStatus({ code: SpanStatusCode.ERROR, message: reason });
                return;
              }
              throw err;
            }
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

export async function publishFieldReport(event: Record<string, unknown>): Promise<void> {
  await ensureProducerConnected();
  const key = String(event.source_ref_id ?? event.hazard_id ?? "");
  await producer.send({
    topic: kafkaConfig.fieldReportsTopic,
    acks: -1,
    messages: [
      {
        key,
        value: JSON.stringify(event),
        headers: injectContextIntoKafkaHeaders(undefined),
      },
    ],
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
        await ensureProducerConnected();
        const baseMessage = createHazardKafkaMessage(hazard);
        const messageWithHeaders: Message = {
          ...baseMessage,
          headers: injectContextIntoKafkaHeaders(baseMessage.headers),
        };

        await producer.send({
          topic: kafkaConfig.outTopic,
          acks: -1,
          messages: [messageWithHeaders],
        });

        const attributes = hazard.attributes as Record<string, unknown> | undefined;
        const isCriticalCoastal = hazard.source === "SENSOR_PANTAI" &&
          hazard.hazard_type === "COASTAL" && attributes?.critical === true;
        if (isCriticalCoastal) {
          await producer.send({
            topic: kafkaConfig.criticalTopic,
            acks: -1,
            messages: [messageWithHeaders],
          });
        }

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
  if (consumerConnected) {
    await consumer.disconnect();
    consumerConnected = false;
  }
  if (producerConnected) {
    await producer.disconnect();
    producerConnected = false;
  }
}
