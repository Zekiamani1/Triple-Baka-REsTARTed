import {
  Kafka,
  Partitioners,
  type Consumer,
  type EachMessagePayload,
  type Message,
  type Producer,
} from "kafkajs";
import { kafka as kafkaConfig } from "./config";

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
  console.log(`Kafka producer connected to ${kafkaConfig.brokers.join(", ")}`);
  console.log(`Kafka consumer listening on ${kafkaConfig.inTopic}`);
}

export async function runIngestConsumer(
  handler: (hazard: Record<string, unknown>) => Promise<void>,
): Promise<void> {
  await consumer.run({
    eachMessage: async ({ message }: EachMessagePayload) => {
      if (!message.value) {
        throw new Error("Received an empty inbound HazardEvent");
      }

      const hazard = JSON.parse(message.value.toString("utf8"));
      if (typeof hazard !== "object" || hazard === null || Array.isArray(hazard)) {
        throw new Error("Inbound HazardEvent must be a JSON object");
      }

      await handler(hazard as Record<string, unknown>);
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
  await producer.send({
    topic: kafkaConfig.outTopic,
    messages: [createHazardKafkaMessage(hazard)],
  });
}

export async function disconnectKafka(): Promise<void> {
  await consumer.disconnect();
  await producer.disconnect();
}
