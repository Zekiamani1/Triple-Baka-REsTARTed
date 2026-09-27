function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

export const kafka = {
  brokers: required('KAFKA_BROKERS', 'localhost:9092').split(','),
  clientId: required('KAFKA_CLIENT_ID', 'bnpb-aggregate-service'),
  outTopic: required('KAFKA_TOPIC_HAZARD_EVENTS', 'hazard-events'),
};
 
export const mongo = {
  uri: required('MONGO_URI', 'mongodb://localhost:27017'),
  dbName: required('MONGO_DB_NAME', 'bnpb'),
  collection: required('MONGO_COLLECTION', 'hazard_events'),
};
 
export const service = {
  port: parseInt(required('AGGREGATOR_PORT', '50051'), 10),
};
