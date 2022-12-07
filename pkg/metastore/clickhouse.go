package metastore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	pb "github.com/parca-dev/parca/gen/proto/go/parca/metastore/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

const (
	dbName                   = "parca"
	tableMetadataMappings    = "metadata_mappings"
	tableMetadataFunctions   = "metadata_functions"
	tableMetadataLocations   = "metadata_locations"
	tableMetadataStacktraces = "metadata_stacktraces"
)

// ClickhouseMetastore is an implementation of the metastore using the clickhouse
// store.
type ClickhouseMetastore struct {
	tracer trace.Tracer
	logger log.Logger

	db clickhouse.Conn

	pb.UnimplementedMetastoreServiceServer
}

var _ pb.MetastoreServiceServer = &ClickhouseMetastore{}

// NewClickhouseMetastore returns a new BadgerMetastore with using clickhouse
// instance.
func NewClickhouseMetastore(
	logger log.Logger,
	reg prometheus.Registerer,
	tracer trace.Tracer,
	db clickhouse.Conn,
) *ClickhouseMetastore {
	return &ClickhouseMetastore{
		db:     db,
		tracer: tracer,
		logger: logger,
	}
}

type identifiable interface {
  Mapping | Function | Location | Stacktrace
	ID() string
}

func distinct[T identifiable](arr []T) []T {
	if len(arr) == 0 {
		return arr
	}
	slow := 1
	for fast := 1; fast < len(arr); fast++ {
    // save latest item
		if arr[fast].ID() != arr[fast-1].ID() {
			arr[slow] = arr[fast]
			slow++
		} else {
			arr[slow] = arr[fast]
    }
	}
	return arr[:slow]
}


type Mapping struct {
	MappingID string `ch:"mapping_id"`
	Payload   string `ch:"payload"`
}

func (m Mapping) ID() string { return m.MappingID }

func (m *ClickhouseMetastore) mappings(ctx context.Context, mappingIDs []string) ([]Mapping, error) {
	query := fmt.Sprintf("SELECT mapping_id, payload FROM %s.%s FINAL WHERE mapping_id IN(@mapping_ids) ", dbName, tableMetadataMappings)
	namedArgs := []interface{}{
		clickhouse.Named("mapping_ids", mappingIDs),
	}
	mappingIndexes := make(map[string]int, len(mappingIDs))
	for i, mappingID := range mappingIDs {
		mappingIndexes[mappingID] = i
	}
	mappings := make([]Mapping, 0, len(mappingIDs))
	err := m.db.Select(ctx, &mappings, query, namedArgs...)
	if err != nil {
		return nil, err
	}

	sort.Slice(mappings, func(i, j int) bool {
		return mappingIndexes[mappings[i].MappingID] < mappingIndexes[mappings[j].MappingID]
	})
	return distinct(mappings), nil
}

func (m *ClickhouseMetastore) Mappings(ctx context.Context, r *pb.MappingsRequest) (*pb.MappingsResponse, error) {
	res := &pb.MappingsResponse{
		Mappings: make([]*pb.Mapping, 0, len(r.MappingIds)),
	}
	if len(r.MappingIds) == 0 {
		return res, nil
	}
	mappings, err := m.mappings(ctx, r.MappingIds)
	if err != nil {
		return nil, err
	}
	for _, i := range mappings {
		mapping := &pb.Mapping{}
		err := mapping.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		res.Mappings = append(res.Mappings, mapping)
	}

	return res, err
}

func (m *ClickhouseMetastore) GetOrCreateMappings(ctx context.Context, r *pb.GetOrCreateMappingsRequest) (*pb.GetOrCreateMappingsResponse, error) {
	res := &pb.GetOrCreateMappingsResponse{
		Mappings: make([]*pb.Mapping, 0, len(r.Mappings)),
	}
	if len(r.Mappings) == 0 {
		return res, nil
	}

	mappingIDs := make([]string, 0, len(r.Mappings))
	for i := range r.Mappings {
		mapping := r.Mappings[i]
		mappingIDs = append(mappingIDs, MakeMappingID(mapping))
	}
	mappings, err := m.mappings(ctx, mappingIDs)
	if err != nil {
		return nil, err
	}
	existIDs := make(map[string]*pb.Mapping, len(mappings))
	for _, i := range mappings {
		mapping := &pb.Mapping{}
		err := mapping.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		existIDs[i.MappingID] = mapping
	}
	if len(mappings) == len(r.Mappings) {
		for _, v := range mappingIDs {
			res.Mappings = append(res.Mappings, existIDs[v])
		}
		return res, nil
	}

	statement, err := m.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableMetadataMappings))
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UnixMilli()
	for i := range r.Mappings {
		mapping := r.Mappings[i]
		id := MakeMappingID(mapping)
		if _, ok := existIDs[id]; !ok {
			mapping.Id = id
			payload, err := mapping.MarshalVT()
			if err != nil {
				return nil, err
			}
			err = statement.Append(
				"0",
				id,
				payload,
				timestamp,
			)
			if err != nil {
				return nil, err
			}
			res.Mappings = append(res.Mappings, mapping)
		} else {
			res.Mappings = append(res.Mappings, existIDs[id])
		}
	}
	if err := statement.Send(); err != nil {
		return nil, err
	}

	return res, err
}

type Function struct {
	// function_id = filename_hash/function_hash
	FunctionID   string `ch:"function_id"`
	FilenameHash string `ch:"filename_hash"`
	FunctionHash string `ch:"function_hash"`
	Payload      string `ch:"payload"`
}

func (f Function) ID() string { return f.FunctionID }

func (m *ClickhouseMetastore) functions(ctx context.Context, functionIDs []string) ([]Function, error) {
	fileNameHashSet := make(map[string]struct{}, 0)
	functionHashSet := make(map[string]struct{}, 0)
	for _, i := range functionIDs {
		filenameHash, functionHash, err := resolveFunctionID(i)
		if err != nil {
			return nil, err
		}
		fileNameHashSet[filenameHash] = struct{}{}
		functionHashSet[functionHash] = struct{}{}
	}
	filenameHashs := make([]string, 0, len(fileNameHashSet))
	for filenameHash := range fileNameHashSet {
		filenameHashs = append(filenameHashs, filenameHash)
	}
	functionHashs := make([]string, 0, len(functionHashSet))
	for functionHash := range functionHashSet {
		functionHashs = append(functionHashs, functionHash)
	}
	namedArgs := []interface{}{
		clickhouse.Named("filename_hashs", filenameHashs),
		clickhouse.Named("function_hashs", functionHashs),
	}
	query := fmt.Sprintf(
		`SELECT 
     concat(filename_hash, '/', function_hash) as function_id, 
     payload 
     FROM %s.%s 
     WHERE oid = '0' AND filename_hash IN(@filename_hashs) AND function_hash IN(@function_hashs)`,
		dbName,
		tableMetadataFunctions,
	)
	functionIndexes := make(map[string]int, len(functionIDs))
	for i, functionID := range functionIDs {
		functionIndexes[functionID] = i
	}
	functions := make([]Function, 0, len(functionIDs))
	err := m.db.Select(ctx, &functions, query, namedArgs...)
	if err != nil {
		return nil, err
	}
	sort.Slice(functions, func(i, j int) bool {
		return functionIndexes[functions[i].FunctionID] < functionIndexes[functions[j].FunctionID]
	})
	return distinct(functions), nil
}

func (m *ClickhouseMetastore) Functions(ctx context.Context, r *pb.FunctionsRequest) (*pb.FunctionsResponse, error) {
	res := &pb.FunctionsResponse{
		Functions: make([]*pb.Function, 0, len(r.FunctionIds)),
	}
	if len(r.FunctionIds) == 0 {
		return res, nil
	}

	functions, err := m.functions(ctx, r.FunctionIds)
	if err != nil {
		return nil, err
	}

	for _, i := range functions {
		function := &pb.Function{}
		err := function.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		res.Functions = append(res.Functions, function)
	}

	return res, err
}

func (m *ClickhouseMetastore) GetOrCreateFunctions(ctx context.Context, r *pb.GetOrCreateFunctionsRequest) (*pb.GetOrCreateFunctionsResponse, error) {
	res := &pb.GetOrCreateFunctionsResponse{
		Functions: make([]*pb.Function, 0, len(r.Functions)),
	}
	if len(r.Functions) == 0 {
		return res, nil
	}
	functionIDs := make([]string, 0, len(r.Functions))
	for i := range r.Functions {
		function := r.Functions[i]
		functionIDs = append(functionIDs, MakeFunctionID(function))
	}
	functions, err := m.functions(ctx, functionIDs)
	if err != nil {
		return nil, err
	}
	existIDs := make(map[string]*pb.Function, len(functions))
	for _, i := range functions {
		function := &pb.Function{}
		err := function.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		existIDs[i.FunctionID] = function
	}
	if len(functions) == len(r.Functions) {
		for _, v := range functionIDs {
			res.Functions = append(res.Functions, existIDs[v])
		}
		return res, nil
	}

	statement, err := m.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableMetadataFunctions))
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UnixMilli()
	for i := range r.Functions {
		function := r.Functions[i]
		functionID := MakeFunctionID(function)
		if _, ok := existIDs[functionID]; !ok {
			function.Id = functionID
			filenameHash, functionHash, err := resolveFunctionID(functionID)
			if err != nil {
				return nil, err
			}
			payload, err := function.MarshalVT()
			if err != nil {
				return nil, err
			}
			err = statement.Append(
				"0",
				filenameHash,
				functionHash,
				payload,
				timestamp,
			)
			if err != nil {
				return nil, err
			}
			res.Functions = append(res.Functions, function)
		} else {
			res.Functions = append(res.Functions, existIDs[functionID])
		}
	}
	if err := statement.Send(); err != nil {
		return nil, err
	}

	return res, err
}

type Location struct {
	LocationID   string `ch:"location_id"`
	MappingID    string `ch:"mapping_id"`
	LocationHash string `ch:"location_hash"`
	Payload      string `ch:"payload"`
}

func (l Location) ID() string { return l.LocationID }

func (m *ClickhouseMetastore) locations(ctx context.Context, locationIDs []string) ([]Location, error) {
	mappingIDSet := make(map[string]struct{}, 0)
	locationHashSet := make(map[string]struct{}, 0)
	for _, i := range locationIDs {
		mappingID, locationHash, err := resolveLocationID(i)
		if err != nil {
			return nil, err
		}
		mappingIDSet[mappingID] = struct{}{}
		locationHashSet[locationHash] = struct{}{}
	}
	mappingIDs := make([]string, 0, len(mappingIDSet))
	for mappingID := range mappingIDSet {
		mappingIDs = append(mappingIDs, mappingID)
	}
	locationHashs := make([]string, 0, len(locationHashSet))
	for locationHash := range locationHashSet {
		locationHashs = append(locationHashs, locationHash)
	}
	namedArgs := []interface{}{
		clickhouse.Named("mapping_ids", mappingIDs),
		clickhouse.Named("location_hashs", locationHashs),
	}
	query := fmt.Sprintf(
		`SELECT 
     concat(mapping_id, '/', location_hash) as location_id, 
     payload 
     FROM %s.%s 
     WHERE oid = '0' AND mapping_id IN(@mapping_ids) AND location_hash IN(@location_hashs)`,
		dbName,
		tableMetadataLocations,
	)
	locationIndexes := make(map[string]int, len(locationIDs))
	for i, locationID := range locationIDs {
		locationIndexes[locationID] = i
	}
	locations := make([]Location, 0, len(locationIDs))
	err := m.db.Select(ctx, &locations, query, namedArgs...)
	if err != nil {
		return nil, err
	}

	sort.Slice(locations, func(i, j int) bool {
		return locationIndexes[locations[i].LocationID] < locationIndexes[locations[j].LocationID]
	})

	return distinct(locations), nil
}

func (m *ClickhouseMetastore) Locations(ctx context.Context, r *pb.LocationsRequest) (*pb.LocationsResponse, error) {
	res := &pb.LocationsResponse{
		Locations: make([]*pb.Location, 0, len(r.LocationIds)),
	}

	if len(r.LocationIds) == 0 {
		return res, nil
	}
	locations, err := m.locations(ctx, r.LocationIds)
	if err != nil {
		return nil, err
	}

	for _, i := range locations {
		location := &pb.Location{}
		err := location.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		res.Locations = append(res.Locations, location)
	}

	return res, err
}

func (m *ClickhouseMetastore) GetOrCreateLocations(ctx context.Context, r *pb.GetOrCreateLocationsRequest) (*pb.GetOrCreateLocationsResponse, error) {
	res := &pb.GetOrCreateLocationsResponse{
		Locations: make([]*pb.Location, 0, len(r.Locations)),
	}
	if len(r.Locations) == 0 {
		return res, nil
	}

	locationIDs := make([]string, 0, len(r.Locations))
	for i := range r.Locations {
		location := r.Locations[i]
		locationIDs = append(locationIDs, MakeLocationID(location))
	}
	locations, err := m.locations(ctx, locationIDs)
	if err != nil {
		return nil, err
	}

	existIDs := make(map[string]*pb.Location, len(locations))
	for _, i := range locations {
		location := &pb.Location{}
		err := location.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		existIDs[i.LocationID] = location
	}
	if len(locations) == len(r.Locations) {
		for _, v := range locationIDs {
			res.Locations = append(res.Locations, existIDs[v])
		}
		return res, nil
	}

	statement, err := m.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableMetadataLocations))
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UnixMilli()
	for i := range r.Locations {
		location := r.Locations[i]
		locationID := MakeLocationID(location)
		if _, ok := existIDs[locationID]; !ok {
			location.Id = locationID
			mappingID, locationHash, err := resolveLocationID(locationID)
			if err != nil {
				return nil, err
			}
			payload, err := location.MarshalVT()
			if err != nil {
				return nil, err
			}
			if location.MappingId != "" && location.Address != 0 && len(location.Lines) == 0 {
				err = statement.Append(
					"0",
					mappingID,
					locationHash,
					payload,
					true,
					timestamp,
				)
				if err != nil {
					return nil, err
				}
			} else {
				err = statement.Append(
					"0",
					mappingID,
					locationHash,
					payload,
					false,
					timestamp,
				)
				if err != nil {
					return nil, err
				}
				res.Locations = append(res.Locations, location)
			}
		} else {
			res.Locations = append(res.Locations, existIDs[locationID])
		}
	}
	if err := statement.Send(); err != nil {
		return nil, err
	}
	return res, err
}

func (m *ClickhouseMetastore) UnsymbolizedLocations(ctx context.Context, r *pb.UnsymbolizedLocationsRequest) (*pb.UnsymbolizedLocationsResponse, error) {
	var locations []*pb.Location

	query := fmt.Sprintf(
		`SELECT 
    concat(mapping_id, '/', location_hash) as location_id, 
    payload 
    FROM %s.%s 
    WHERE unsymbolized = @unsymbolized`,
		dbName,
		tableMetadataLocations)

	namedArgs := []interface{}{
		clickhouse.Named("unsymbolized", true),
	}

	if r.MinKey != "" {
		minLocationID := LocationIDFromUnsymbolizedKey(r.MinKey)
		mappingID, locationHash, err := resolveLocationID(minLocationID)
		if err != nil {
			return nil, err
		}
		namedArgs = append(namedArgs, clickhouse.Named("mapping_id", mappingID))
		query += " AND mapping_id=@mapping_id"
		namedArgs = append(namedArgs, clickhouse.Named("location_hash", locationHash))
		query += " AND location_hash>=@location_hash"
	}
	if r.Limit > 0 {
		namedArgs = append(namedArgs, clickhouse.Named("limit", r.Limit))
		query += " LIMIT @limit"
	}

	var unsymbolizedLocations []Location

	err := m.db.Select(ctx, &unsymbolizedLocations, query, namedArgs...)
	if err != nil {
		return nil, err
	}
	maxKey := ""
	for _, i := range unsymbolizedLocations {
		maxKey = MakeLocationKeyWithID(i.LocationID)
		location := &pb.Location{}
		err := location.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		locations = append(locations, location)
	}

	return &pb.UnsymbolizedLocationsResponse{
		Locations: locations,
		MaxKey:    maxKey,
	}, nil
}

func (m *ClickhouseMetastore) CreateLocationLines(ctx context.Context, r *pb.CreateLocationLinesRequest) (*pb.CreateLocationLinesResponse, error) {
	statement, err := m.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableMetadataLocations))
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().UnixMilli()
	for i := range r.Locations {
		location := r.Locations[i]
		locationID := MakeLocationID(location)
		mappingID, locationHash, err := resolveLocationID(locationID)
		if err != nil {
			return nil, err
		}
		location.Id = locationID
		payload, err := location.MarshalVT()
		if err != nil {
			return nil, err
		}
		err = statement.Append(
			"0",
			mappingID,
			locationHash,
			payload,
			false,
			timestamp,
		)
		if err != nil {
			return nil, err
		}
	}
	if err := statement.Send(); err != nil {
		return nil, err
	}

	return &pb.CreateLocationLinesResponse{}, nil
}

type Stacktrace struct {
	StacktraceID     string `ch:"stacktrace_id"`
	MappingID        string `ch:"mapping_id"`
	LocationHash     string `ch:"location_id"`
	LoacationIDsHash string `ch:"location_ids_hash"`
	Payload          string `ch:"payload"`
}

func (s Stacktrace) ID() string { return s.StacktraceID }

func (m *ClickhouseMetastore) stacktraces(ctx context.Context, stacktraceIDs []string) ([]Stacktrace, error) {
	mappingIDSet := make(map[string]struct{}, 0)
	locationHashSet := make(map[string]struct{}, 0)
	locationIDsHashSet := make(map[string]struct{}, 0)

	for _, stacktraceID := range stacktraceIDs {
		mappingID, locationHash, locationIDsHash, err := resolveStacktraceID(stacktraceID)
		if err != nil {
			return nil, err
		}
		mappingIDSet[mappingID] = struct{}{}
		locationHashSet[locationHash] = struct{}{}
		locationIDsHashSet[locationIDsHash] = struct{}{}
	}
	mappingIDs := make([]string, 0, len(mappingIDSet))
	locationHashs := make([]string, 0, len(locationHashSet))
	locationIDsHashs := make([]string, 0, len(locationIDsHashSet))
	for mappingID := range mappingIDSet {
		mappingIDs = append(mappingIDs, mappingID)
	}
	for locationHash := range locationHashSet {
		locationHashs = append(locationHashs, locationHash)
	}
	for locationIDsHash := range locationIDsHashSet {
		locationIDsHashs = append(locationIDsHashs, locationIDsHash)
	}

	namedArgs := []interface{}{
		clickhouse.Named("mapping_ids", mappingIDs),
		clickhouse.Named("location_hashs", locationHashs),
		clickhouse.Named("location_ids_hashs", locationIDsHashs),
	}
	query := fmt.Sprintf(
		`SELECT
    concat(mapping_id, '/', location_hash, '/', location_ids_hash) as stacktrace_id,
    payload
    FROM %s.%s 
    WHERE
    oid='0' AND mapping_id IN (@mapping_ids) AND location_hash IN (@location_hashs) AND location_ids_hash IN (@location_ids_hashs) 
    `,
		dbName,
		tableMetadataStacktraces,
	)

	stacktraceIndexes := make(map[string]int, len(stacktraceIDs))
	for i, stacktraceID := range stacktraceIDs {
		if stacktraceID == "empty-stacktrace" {
			stacktraceID = "unknown-mapping/unknown-location_hash/unknown-location_ids_hash"
		}
		stacktraceIndexes[stacktraceID] = i
	}

	stacktraces := make([]Stacktrace, 0, len(stacktraceIDs))
	err := m.db.Select(ctx, &stacktraces, query, namedArgs...)
	if err != nil {
		return nil, err
	}
	sort.Slice(stacktraces, func(i, j int) bool {
		return stacktraceIndexes[stacktraces[i].StacktraceID] < stacktraceIndexes[stacktraces[j].StacktraceID]
	})
	return distinct(stacktraces), nil
}
func (m *ClickhouseMetastore) Stacktraces(ctx context.Context, r *pb.StacktracesRequest) (*pb.StacktracesResponse, error) {
	res := &pb.StacktracesResponse{
		Stacktraces: make([]*pb.Stacktrace, 0, len(r.StacktraceIds)),
	}
	if len(r.StacktraceIds) == 0 {
		return res, nil
	}

	stacktraces, err := m.stacktraces(ctx, r.StacktraceIds)
	if err != nil {
		return nil, err
	}

	for _, i := range stacktraces {
		stacktrace := &pb.Stacktrace{}
		err := stacktrace.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		res.Stacktraces = append(res.Stacktraces, stacktrace)
	}

	return res, err
}

func (m *ClickhouseMetastore) GetOrCreateStacktraces(ctx context.Context, r *pb.GetOrCreateStacktracesRequest) (*pb.GetOrCreateStacktracesResponse, error) {
	res := &pb.GetOrCreateStacktracesResponse{
		Stacktraces: make([]*pb.Stacktrace, 0, len(r.Stacktraces)),
	}
	if len(r.Stacktraces) == 0 {
		return res, nil
	}

	stacktraceIDs := make([]string, 0, len(r.Stacktraces))
	for i := range r.Stacktraces {
		stacktrace := r.Stacktraces[i]
		stacktraceIDs = append(stacktraceIDs, MakeStacktraceID(stacktrace))
	}
	stacktraces, err := m.stacktraces(ctx, stacktraceIDs)
	if err != nil {
		return nil, err
	}

	existIDs := make(map[string]*pb.Stacktrace, len(stacktraces))
	for _, i := range stacktraces {
		stacktrace := &pb.Stacktrace{}
		err := stacktrace.UnmarshalVT([]byte(i.Payload))
		if err != nil {
			return nil, err
		}
		existIDs[i.StacktraceID] = stacktrace
	}
	if len(stacktraces) == len(r.Stacktraces) {
		for _, v := range stacktraces {
			res.Stacktraces = append(res.Stacktraces, existIDs[v.StacktraceID])
		}
		return res, nil
	}

	timestamp := time.Now().UnixMilli()
	statement, err := m.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableMetadataStacktraces))
	if err != nil {
		return nil, err
	}

	for i := range r.Stacktraces {
		stacktrace := r.Stacktraces[i]
		stacktraceID := MakeStacktraceID(stacktrace)
		if _, ok := existIDs[stacktraceID]; !ok {
			stacktrace.Id = stacktraceID
			mappingID, locationHash, locationIDsHash, err := resolveStacktraceID(stacktraceID)
			if err != nil {
				return nil, err
			}
			payload, err := stacktrace.MarshalVT()
			if err != nil {
				return nil, err
			}
			err = statement.Append(
				"0",
				mappingID,
				locationHash,
				locationIDsHash,
				payload,
				timestamp,
			)
			if err != nil {
				return nil, err
			}
			res.Stacktraces = append(res.Stacktraces, stacktrace)
		} else {
			res.Stacktraces = append(res.Stacktraces, existIDs[stacktraceID])
		}
	}
	if err := statement.Send(); err != nil {
		return nil, err
	}
	return res, err
}
