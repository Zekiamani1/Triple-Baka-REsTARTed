import { Kafka } from "kafkajs";
import { kafkaConfig } from "./config";
import { parseHazardMessage } from "./message";

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
});

const consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

async function main(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({
    topic: kafkaConfig.topic,
    fromBeginning: false,
  });

  console.log(`Kafka consumer connected to ${kafkaConfig.brokers.join(", ")}`);
  console.log(
    `Listening to ${kafkaConfig.topic} as group ${kafkaConfig.groupId}`,
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = parseHazardMessage(message.value);

        console.log("HazardEvent received", {
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString(),
          event,
        });
      } catch (error) {
        console.error("Rejected Kafka message", {
          topic,
          partition,
          offset: message.offset,
          error: error instanceof Error ? error.message : error,
        });
      }
    },
  });
}

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  console.log(`Received ${signal}; disconnecting Kafka consumer`);
  await consumer.disconnect();
  process.exit(0);
}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

main().catch((error) => {
  console.error("Event Consumer failed", error);
  process.exit(1);
});
