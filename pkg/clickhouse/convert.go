package clickhouse

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	pb "github.com/parca-dev/parca/gen/proto/go/parca/metastore/v1alpha1"
	"github.com/parca-dev/parca/pkg/profile"
)

type ProfileConverter struct {
	tracer trace.Tracer
	m      pb.MetastoreServiceClient
}

func NewProfileConverter(
	tracer trace.Tracer,
	m pb.MetastoreServiceClient,
) *ProfileConverter {
	return &ProfileConverter{
		tracer: tracer,
		m:      m,
	}
}

func (c *ProfileConverter) Convert(
	ctx context.Context,
	stacktraces []Stacktrace,
	meta profile.Meta,
) (*profile.Profile, error) {
	ctx, span := c.tracer.Start(ctx, "convert-clickhouse-record-to-profile")
	defer span.End()

	stacktraceIDs := make([]string, len(stacktraces))
	for i := 0; i < len(stacktraces); i++ {
		stacktraceIDs[i] = stacktraces[i].StacktraceID
	}

	stacktraceLocations, err := c.resolveStacktraces(ctx, stacktraceIDs)
	if err != nil {
		return nil, fmt.Errorf("read stacktrace metadata: %w", err)
	}

	samples := make([]*profile.SymbolizedSample, 0, len(stacktraces))
	for i := 0; i < len(stacktraces); i++ {
		samples = append(samples, &profile.SymbolizedSample{
			Value:     stacktraces[i].Value,
			Locations: stacktraceLocations[i],
		})
	}

	return &profile.Profile{
		Samples: samples,
		Meta:    meta,
	}, nil
}

func (c *ProfileConverter) resolveStacktraces(ctx context.Context, stacktraceIDs []string) ([][]*profile.Location, error) {
	ctx, span := c.tracer.Start(ctx, "resolve-stacktraces")
	defer span.End()

	sres, err := c.m.Stacktraces(ctx, &pb.StacktracesRequest{
		StacktraceIds: stacktraceIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("read stacktraces: %w", err)
	}

	locationNum := 0
	for _, stacktrace := range sres.Stacktraces {
		stacktrace := stacktrace
		locationNum += len(stacktrace.LocationIds)
	}

	locationIndex := make(map[string]int, locationNum)
	locationIDs := make([]string, 0, locationNum)
	for _, stacktrace := range sres.Stacktraces {
		stacktrace := stacktrace
		for _, id := range stacktrace.LocationIds {
			if _, seen := locationIndex[id]; !seen {
				locationIDs = append(locationIDs, id)
				locationIndex[id] = len(locationIDs) - 1
			}
		}
	}

	lres, err := c.m.Locations(ctx, &pb.LocationsRequest{LocationIds: locationIDs})
	if err != nil {
		return nil, err
	}

	locations, err := c.getLocationsFromSerializedLocations(ctx, locationIDs, lres.Locations)
	if err != nil {
		return nil, err
	}

	stacktraceLocations := make([][]*profile.Location, len(sres.Stacktraces))
	for i, stacktrace := range sres.Stacktraces {
		stacktrace := stacktrace
		stacktraceLocations[i] = make([]*profile.Location, len(stacktrace.LocationIds))
		for j, id := range stacktrace.LocationIds {
			stacktraceLocations[i][j] = locations[locationIndex[id]]
		}
	}

	return stacktraceLocations, nil
}

func (c *ProfileConverter) getLocationsFromSerializedLocations(
	ctx context.Context,
	locationIds []string,
	locations []*pb.Location,
) (
	[]*profile.Location,
	error,
) {
	mappingIndex := map[string]int{}
	mappingIDs := []string{}
	for _, location := range locations {
		location := location
		if location.MappingId == "" {
			continue
		}

		if _, found := mappingIndex[location.MappingId]; !found {
			mappingIDs = append(mappingIDs, location.MappingId)
			mappingIndex[location.MappingId] = len(mappingIDs) - 1
		}
	}

	var mappings []*pb.Mapping
	if len(mappingIDs) > 0 {
		mres, err := c.m.Mappings(ctx, &pb.MappingsRequest{
			MappingIds: mappingIDs,
		})
		if err != nil {
			return nil, fmt.Errorf("get mappings by IDs: %w", err)
		}
		mappings = mres.Mappings
	}

	functionIndex := map[string]int{}
	functionIDs := []string{}
	for _, location := range locations {
		location := location
		if location.Lines == nil {
			continue
		}
		for _, line := range location.Lines {
			line := line
			if _, found := functionIndex[line.FunctionId]; !found {
				functionIDs = append(functionIDs, line.FunctionId)
				functionIndex[line.FunctionId] = len(functionIDs) - 1
			}
		}
	}

	fres, err := c.m.Functions(ctx, &pb.FunctionsRequest{
		FunctionIds: functionIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("get functions by ids: %w", err)
	}

	res := make([]*profile.Location, 0, len(locations))
	for _, location := range locations {
		location := location
		var mapping *pb.Mapping
		if location.MappingId != "" {
			mapping = mappings[mappingIndex[location.MappingId]]
		}

		symbolizedLines := []profile.LocationLine{}
		if location.Lines != nil {
			lines := location.Lines
			symbolizedLines = make([]profile.LocationLine, 0, len(lines))
			for _, line := range lines {
				line := line
				symbolizedLines = append(symbolizedLines, profile.LocationLine{
					Function: fres.Functions[functionIndex[line.FunctionId]],
					Line:     line.Line,
				})
			}
		}

		res = append(res, &profile.Location{
			ID:       location.Id,
			Address:  location.Address,
			IsFolded: location.IsFolded,
			Mapping:  mapping,
			Lines:    symbolizedLines,
		})
	}

	return res, nil
}
