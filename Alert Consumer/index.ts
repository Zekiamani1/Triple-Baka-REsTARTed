import { Kafka } from "kafkajs";
import { kafkaConfig } from "./config";
import { isEmergency, parseHazardMessage } from "./message";

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
});

const consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

async function main(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: true });
  console.log(`Listening to ${kafkaConfig.topic} for emergency events`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = parseHazardMessage(message.value);
      if (isEmergency(event)) console.log("EMERGENCY ALERT", event);
    },
  });
}

async function shutdown(): Promise<void> {
  await consumer.disconnect();
  process.exit(0);
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
main().catch(console.error);
