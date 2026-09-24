  | "BMKG"
  | "PVMBG";

export type HazardType =
  | "SEISMIC"
  | "VOLCANIC";

export type HazardSeverity =
  | "NORMAL"
  | "WASPADA"
  | "SIAGA"
  | "AWAS";

export interface HazardEvent {
    hazard_id: string;
    source: Source;
    source_ref_id: string;
    hazard_type: HazardType;
    severity: Severity;
    area_name: string;
    latitude: number;
    longitude: number;
    occurred_at: string;
    ingested_at: string;
    attributes: Record<string, unknown>; 
}