export const kafka = {
  brokers: required('KAFKA_BROKERS', 'localhost:9092').split(','),
  clientId: required('KAFKA_CLIENT_ID', 'bnpb-aggregate-service'),
  inTopic: required('KAFKA_TOPIC_CANONICAL_RAW', 'canonical-events-raw'),
  outTopic: required('KAFKA_TOPIC_HAZARD_EVENTS', 'hazard-events'),
  consumerGroupId: required('KAFKA_CONSUMER_GROUP', 'aggregate-group'),
};
 
export const mongo = {
  uri: required('MONGO_URI', 'mongodb://localhost:27017'),
  dbName: required('MONGO_DB_NAME', 'bnpb'),
  collection: required('MONGO_COLLECTION', 'hazard_events'),
};
 
export const service = {
  port: parseInt(required('AGGREGATOR_PORT', '8090'), 10),
};