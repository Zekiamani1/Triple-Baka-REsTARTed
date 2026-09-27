import {
  Kafka,
  Partitioners,
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

export async function connectKafka(): Promise<void> {
  await producer.connect();
  console.log(`Kafka producer connected to ${kafkaConfig.brokers.join(", ")}`);
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
  await producer.disconnect();
}
