import { Kafka } from "kafkajs";
import { createServer } from "node:http";
import { kafkaConfig } from "./config";
import { parseHazardMessage } from "./message";

const kafka = new Kafka({
  clientId: kafkaConfig.clientId,
  brokers: kafkaConfig.brokers,
});

const consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

function startHealthServer(): void {
  const port = Number(process.env.HEALTH_PORT ?? "8087");
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
  await consumer.subscribe({
    topic: kafkaConfig.topic,
    fromBeginning: true,
  });
  console.log(`Listening to ${kafkaConfig.topic} for all events`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("HAZARD EVENT", parseHazardMessage(message.value));
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
