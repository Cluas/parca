package clickhouse

const (
	dbName = "parca"
	// The columns are sorted by their name in the schema too.
	tableProfiles      = "profiles"
	tableProfileTypes  = "profile_types"
	tableProfileLabels = "profile_labels"
	tableProfilesInput = "profiles_input"
	tableStacktraces   = "profile_stacktraces"
)

type Profile struct {
	Name       string     `ch:"name"`
	Period     string     `ch:"period"`
	PeriodType string     `ch:"period_type"`
	PeriodUnit string     `ch:"period_unit"`
	SampleType string     `ch:"sample_type"`
	SampleUnit string     `ch:"sample_unit"`
	Duration   int64      `ch:"duration"`
	Labels     [][]string `ch:"labels"`
	Timestamp  int64      `ch:"timestamp_ms"`
	// total value for this profile
	Value int64 `ch:"value"`
}

type Stacktrace struct {
	StacktraceID string `ch:"stacktrace_id"`
	Value        int64  `ch:"value"`
}

type ProfileType struct {
	Name       string `ch:"name"`
	PeriodType string `ch:"period_type"`
	PeriodUnit string `ch:"period_unit"`
	SampleType string `ch:"sample_type"`
	SampleUnit string `ch:"sample_unit"`
	Delta      bool   `ch:"delta"`
}
