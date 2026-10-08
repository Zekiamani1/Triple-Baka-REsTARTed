function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export const kafkaConfig = {
  brokers: required("KAFKA_BROKERS", "localhost:9092").split(","),
  clientId: required("KAFKA_CLIENT_ID", "alert-consumer"),
  groupId: required("KAFKA_CONSUMER_GROUP", "emergency-notifier"),
  topic: required("KAFKA_TOPIC_HAZARD_EVENTS", "hazard-events"),
};
