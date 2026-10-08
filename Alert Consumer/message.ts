export type HazardMessage = Record<string, unknown>;

export function parseHazardMessage(value: Buffer | null): HazardMessage {
  if (!value) throw new Error("Empty Kafka message");
  return JSON.parse(value.toString()) as HazardMessage;
}

export function isEmergency(event: HazardMessage): boolean {
  return event.severity === "SIAGA" || event.severity === "AWAS";
}
