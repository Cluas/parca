package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	metastorepb "github.com/parca-dev/parca/gen/proto/go/parca/metastore/v1alpha1"
	profilestorepb "github.com/parca-dev/parca/gen/proto/go/parca/profilestore/v1alpha1"
	pb "github.com/parca-dev/parca/gen/proto/go/parca/query/v1alpha1"
	"github.com/parca-dev/parca/pkg/profile"
)

func NewQuerier(
	tracer trace.Tracer,
	db clickhouse.Conn,
	metastore metastorepb.MetastoreServiceClient,
) *Querier {
	return &Querier{
		tracer: tracer,
		db:     db,
		converter: NewProfileConverter(
			tracer,
			metastore,
		),
	}
}

type Querier struct {
	db        clickhouse.Conn
	converter *ProfileConverter
	tracer    trace.Tracer
}

func (q *Querier) Labels(ctx context.Context, match []string, start, end time.Time) ([]string, error) {
	query := fmt.Sprintf("SELECT groupUniqArray(key) FROM %s.%s WHERE oid='0'", dbName, tableProfileLabels)

	var labels []string
	if err := q.db.QueryRow(ctx, query).Scan(&labels); err != nil {
		return nil, err
	}

	sort.Strings(labels)

	return labels, nil
}

func (q *Querier) Values(ctx context.Context, labelName string, match []string, start, end time.Time) ([]string, error) {
	exprs := []string{
		"key=@key",
	}
	args := []interface{}{
		clickhouse.Named("key", labelName),
	}
	whereExpr := strings.Join(exprs, " AND ")
	query := fmt.Sprintf("SELECT groupUniqArray(value) FROM %s.%s WHERE oid='0' AND %s", dbName, tableProfileLabels, whereExpr)
	var values []string
	if err := q.db.QueryRow(ctx, query, args...).Scan(&values); err != nil {
		return nil, err
	}

	sort.Strings(values)

	return values, nil
}

func MatcherToBooleanExpressionString(matcher *labels.Matcher) (string, error) {
	switch matcher.Type {
	case labels.MatchEqual:
		return fmt.Sprintf("key='%s' AND value='%s'", matcher.Name, matcher.Value), nil
	case labels.MatchNotEqual:
		return fmt.Sprintf("key='%s' AND value!='%s'", matcher.Name, matcher.Value), nil
	case labels.MatchRegexp:
		return fmt.Sprintf("key='%s' AND match(value, '%s')", matcher.Name, matcher.Value), nil
	case labels.MatchNotRegexp:
		return fmt.Sprintf("key='%s' AND not match(value, '%s')", matcher.Name, matcher.Value), nil
	default:
		return "", fmt.Errorf("unsupported matcher type %v", matcher.Type.String())
	}
}

func MatchersToBooleanExprsAndArgs(matchers []*labels.Matcher, profileType string) ([]string, []interface{}, error) {
	if len(matchers) == 0 {
		return nil, nil, nil
	}
	subQuery := fmt.Sprintf(`SELECT timestamp_ms, profile_id FROM parca.profile_ref_labels WHERE oid='0' AND profile_type = '%s' AND `, profileType)
	exprs := make([]string, 0, len(matchers))

	for _, matcher := range matchers {
		expr, err := MatcherToBooleanExpressionString(matcher)
		if err != nil {
			return nil, nil, err
		}

		expr = subQuery + expr
		exprs = append(exprs, expr)
	}
	return []string{fmt.Sprintf("(timestamp_ms, profile_id) IN (%s)", strings.Join(exprs, " INTERSECT "))}, nil, nil
}

func QueryToFilterExprsAndArgs(query string) (string, profile.Meta, []string, []interface{}, error) {
	parsedSelector, err := parser.ParseMetricSelector(query)
	if err != nil {
		return "", profile.Meta{}, nil, nil, status.Error(codes.InvalidArgument, "failed to parse query")
	}

	sel := make([]*labels.Matcher, 0, len(parsedSelector))
	var nameLabel *labels.Matcher
	for _, matcher := range parsedSelector {
		if matcher.Name == labels.MetricName {
			nameLabel = matcher
		} else {
			sel = append(sel, matcher)
		}
	}
	if nameLabel == nil {
		return "", profile.Meta{}, nil, nil, status.Error(codes.InvalidArgument, "query must contain a profile-type selection")
	}

	parts := strings.Split(nameLabel.Value, ":")
	if len(parts) != 5 && len(parts) != 6 {
		return "", profile.Meta{}, nil, nil, status.Errorf(codes.InvalidArgument, "profile-type selection must be of the form <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta), got(%d): %q", len(parts), nameLabel.Value)
	}
	name, sampleType, sampleUnit, periodType, periodUnit, delta := parts[0], parts[1], parts[2], parts[3], parts[4], false
	if len(parts) == 6 && parts[5] == "delta" {
		delta = true
	}
	profileType := strings.Join(parts[:5], ":")
	if delta {
		profileType += ":delta"
	}

	labelFilterExpressions, labelFilterArgs, err := MatchersToBooleanExprsAndArgs(sel, profileType)
	if err != nil {
		return "", profile.Meta{}, nil, nil, status.Error(codes.InvalidArgument, "failed to build query")
	}
	exprs := []string{
		"profile_type=@profile_type",
	}
	args := []interface{}{
		clickhouse.Named("profile_type", profileType),
	}
	exprs = append(exprs, labelFilterExpressions...)
	args = append(args, labelFilterArgs...)

	return profileType, profile.Meta{
		Name:       name,
		SampleType: profile.ValueType{Type: sampleType, Unit: sampleUnit},
		PeriodType: profile.ValueType{Type: periodType, Unit: periodUnit},
	}, exprs, args, nil
}

func (q *Querier) QueryRange(
	ctx context.Context,
	query string,
	startTime, endTime time.Time,
	limit uint32,
) ([]*pb.MetricsSeries, error) {
	_, _, selectorExprs, selectorArgs, err := QueryToFilterExprsAndArgs(query)
	if err != nil {
		return nil, err
	}

	start := timestamp.FromTime(startTime)
	end := timestamp.FromTime(endTime)

	selectorExprs = append(selectorExprs, "timestamp_ms > @start")
	selectorExprs = append(selectorExprs, "timestamp_ms < @end")
	selectorArgs = append(selectorArgs, clickhouse.Named("start", start))
	selectorArgs = append(selectorArgs, clickhouse.Named("end", end))

	whereExpr := strings.Join(selectorExprs, " AND ")

	resSeries := []*pb.MetricsSeries{}
	labelsetToIndex := map[string]int{}
	labelSet := labels.Labels{}

	query = fmt.Sprintf("SELECT value, labels, timestamp_ms FROM %s.%s WHERE oid='0' AND %s ORDER BY timestamp_ms", dbName, tableProfiles, whereExpr)

	var profiles []Profile
	if err := q.db.Select(ctx, &profiles, query, selectorArgs...); err != nil {
		return nil, err
	}
	if len(profiles) == 0 {
		return nil, status.Error(
			codes.NotFound,
			"No data found for the query, try a different query or time range or no data has been written to be queried yet.",
		)
	}

	for i := 0; i < len(profiles); i++ {
		labelSet = labelSet[:0]
		for _, l := range profiles[i].Labels {
			labelSet = append(labelSet, labels.Label{Name: l[0], Value: l[1]})
		}
		sort.Sort(labelSet)
		s := labelSet.String()
		index, ok := labelsetToIndex[s]
		if !ok {
			pbLabelSet := make([]*profilestorepb.Label, 0, len(labelSet))
			for _, l := range labelSet {
				pbLabelSet = append(pbLabelSet, &profilestorepb.Label{
					Name:  l.Name,
					Value: l.Value,
				})
			}
			resSeries = append(resSeries, &pb.MetricsSeries{Labelset: &profilestorepb.LabelSet{Labels: pbLabelSet}})
			index = len(resSeries) - 1
			labelsetToIndex[s] = index
		}

		series := resSeries[index]
		series.Samples = append(series.Samples, &pb.MetricsSample{
			Timestamp: timestamppb.New(timestamp.Time(profiles[i].Timestamp)),
			Value:     profiles[i].Value,
		})
	}

	return resSeries, nil
}

func (q *Querier) ProfileTypes(
	ctx context.Context,
) ([]*pb.ProfileType, error) {
	res := []*pb.ProfileType{}

	var profileTypes []ProfileType
	query := fmt.Sprintf("SELECT DISTINCT name, sample_type, sample_unit, period_type, period_unit, delta FROM %s.%s WHERE oid='0'", dbName, tableProfileTypes)
	if err := q.db.Select(ctx, &profileTypes, query); err != nil {
		return nil, err
	}

	for i := 0; i < len(profileTypes); i++ {
		profileType := profileTypes[i]
		res = append(res, &pb.ProfileType{
			Name:       profileType.Name,
			SampleType: profileType.SampleType,
			SampleUnit: profileType.SampleUnit,
			PeriodType: profileType.PeriodType,
			PeriodUnit: profileType.PeriodUnit,
			Delta:      profileType.Delta,
		})
	}

	return res, nil
}

func (q *Querier) recordToProfile(
	ctx context.Context,
	stacktraces []Stacktrace,
	meta profile.Meta,
) (*profile.Profile, error) {
	ctx, span := q.tracer.Start(ctx, "Querier/arrowRecordToProfile")
	defer span.End()
	return q.converter.Convert(
		ctx,
		stacktraces,
		meta,
	)
}

func (q *Querier) QuerySingle(
	ctx context.Context,
	query string,
	time time.Time,
) (*profile.Profile, error) {
	ctx, span := q.tracer.Start(ctx, "Querier/QuerySingle")
	defer span.End()

	stacktraces, meta, err := q.findSingle(ctx, query, time)
	if err != nil {
		return nil, err
	}

	p, err := q.recordToProfile(
		ctx,
		stacktraces,
		meta,
	)

	if p == nil || err != nil {
		return nil, status.Error(codes.NotFound, "could not find profile at requested time and selectors")
	}

	return p, nil
}

func (q *Querier) findSingle(ctx context.Context, query string, t time.Time) ([]Stacktrace, profile.Meta, error) {
	requestedTime := timestamp.FromTime(t)

	ctx, span := q.tracer.Start(ctx, "Querier/findSingle")
	span.SetAttributes(attribute.String("query", query))
	span.SetAttributes(attribute.Int64("time", t.Unix()))
	defer span.End()

	profileType, meta, selectorExprs, selectorArgs, err := QueryToFilterExprsAndArgs(query)
	if err != nil {
		return nil, profile.Meta{}, err
	}
	selectorExprs = append(selectorExprs, "timestamp_ms=@timestamp_ms")
	selectorExprs = append(selectorExprs, "profile_type=@profile_type")
	selectorArgs = append(selectorArgs, clickhouse.Named("timestamp_ms", requestedTime))
	selectorArgs = append(selectorArgs, clickhouse.Named("profile_type", profileType))
	whereExpr := strings.Join(selectorExprs, " AND ")

	var stacktraces []Stacktrace
	query = fmt.Sprintf(
		`SELECT 
     stacktrace_id, sum(value) as value 
		 FROM %s.%s
		 WHERE oid='0' AND %s 
		 GROUP BY stacktrace_id`,
		dbName,
		tableStacktraces,
		whereExpr,
	)
	if err := q.db.Select(ctx, &stacktraces, query, selectorArgs...); err != nil {
		return nil, profile.Meta{}, err
	}

	return stacktraces,
		profile.Meta{
			Name:       meta.Name,
			SampleType: meta.SampleType,
			PeriodType: meta.PeriodType,
			Timestamp:  requestedTime,
		},
		nil
}

func (q *Querier) QueryMerge(ctx context.Context, query string, start, end time.Time) (*profile.Profile, error) {
	ctx, span := q.tracer.Start(ctx, "Querier/QueryMerge")
	defer span.End()

	stacktraces, meta, err := q.selectMerge(ctx, query, start, end)
	if err != nil {
		return nil, err
	}

	p, err := q.recordToProfile(
		ctx,
		stacktraces,
		meta,
	)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (q *Querier) selectMerge(ctx context.Context, query string, startTime, endTime time.Time) ([]Stacktrace, profile.Meta, error) {
	ctx, span := q.tracer.Start(ctx, "Querier/selectMerge")
	defer span.End()

	profileType, meta, selectorExprs, selectorArgs, err := QueryToFilterExprsAndArgs(query)
	if err != nil {
		return nil, profile.Meta{}, err
	}

	start := timestamp.FromTime(startTime)
	end := timestamp.FromTime(endTime)

	selectorExprs = append(selectorExprs, "timestamp_ms > @start")
	selectorExprs = append(selectorExprs, "timestamp_ms < @end")
	selectorExprs = append(selectorExprs, "profile_type=@profile_type")
	selectorArgs = append(selectorArgs, clickhouse.Named("start", start))
	selectorArgs = append(selectorArgs, clickhouse.Named("end", end))
	selectorArgs = append(selectorArgs, clickhouse.Named("profile_type", profileType))

	whereExpr := strings.Join(selectorExprs, " AND ")

	query = fmt.Sprintf(
		`SELECT 
	   stacktrace_id, sum(value) as value 
		 FROM %s.%s
		 WHERE oid='0' AND %s 
		 GROUP BY stacktrace_id`,
		dbName,
		tableStacktraces,
		whereExpr,
	)

	var stacktraces []Stacktrace
	if err := q.db.Select(ctx, &stacktraces, query, selectorArgs...); err != nil {
		return nil, profile.Meta{}, err
	}

	meta = profile.Meta{
		Name:       meta.Name,
		SampleType: meta.SampleType,
		PeriodType: meta.PeriodType,
		Timestamp:  start,
	}
	return stacktraces, meta, nil
}
