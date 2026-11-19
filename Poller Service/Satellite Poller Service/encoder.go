package main

import (
	"fmt"
	"strings"

	"satellitepoller/domain"
	"satellitepoller/hazardpb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func toProtoHazard(
	event domain.HazardEvent,
) (*hazardpb.Hazard, error) {

	if event.HazardID == "" {
		return nil, fmt.Errorf("hazard_id is required")
	}

	if event.SourceRefID == "" {
		return nil, fmt.Errorf("source_ref_id is required")
	}

	if event.OccurredAt.IsZero() || event.IngestedAt.IsZero() {
		return nil, fmt.Errorf("hazard timestamps are required")
	}

	if event.Source != "SATELIT" {
		return nil, fmt.Errorf("unexpected source: %s", event.Source)
	}

	if event.HazardType != "WILDFIRE" {
		return nil, fmt.Errorf("unexpected hazard type: %s", event.HazardType)
	}

	var severity hazardpb.Severity

	switch strings.ToUpper(event.Severity) {
	case "NORMAL":
		severity = hazardpb.Severity_NORMAL
	case "WASPADA":
		severity = hazardpb.Severity_WASPADA
	case "SIAGA":
		severity = hazardpb.Severity_SIAGA
	case "AWAS":
		severity = hazardpb.Severity_AWAS
	default:
		return nil, fmt.Errorf(
			"invalid severity: %s", event.Severity,
		)
	}

	attributes, err := structpb.NewStruct(event.Attributes)
	if err != nil {
		return nil, fmt.Errorf("convert attributes: %w", err)
	}

	occurredAt := timestamppb.New(event.OccurredAt.UTC())
	ingestedAt := timestamppb.New(event.IngestedAt.UTC())

	if err := occurredAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid occurred_at: %w", err)
	}

	if err := ingestedAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid ingested_at: %w", err)
	}

	return &hazardpb.Hazard{
		HazardId:    event.HazardID,
		Source:      hazardpb.Source_SATELIT,
		SourceRefId: event.SourceRefID,
		HazardType:  hazardpb.HazardType_WILDFIRE,
		Severity:    severity,
		AreaName:    event.AreaName,
		Latitude:    event.Latitude,
		Longitude:   event.Longitude,
		OccurredAt:  occurredAt,
		IngestedAt:  ingestedAt,
		Attributes:  attributes,
	}, nil
}
