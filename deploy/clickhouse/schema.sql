CREATE TABLE IF NOT EXISTS parca.metadata_mappings
(
    `oid` String DEFAULT '0',
    `mapping_id` String,
    `payload` String,
    `timestamp_ms` Int64 CODEC(DoubleDelta)
)
ENGINE = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY oid
ORDER BY (oid, mapping_id);

CREATE TABLE IF NOT EXISTS parca.metadata_functions
(
    `oid` String DEFAULT '0',
    `filename_hash` String,
    `function_hash` String,
    `payload` String,
    `timestamp_ms` Int64 CODEC(DoubleDelta)
)
ENGINE = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))))
ORDER BY (oid, filename_hash, function_hash);

CREATE TABLE IF NOT EXISTS parca.metadata_locations
(
    `oid` String DEFAULT '0',
    `mapping_id` String,
    `location_hash` String,
    `payload` String,
    `unsymbolized` Bool,
    `timestamp_ms` Int64 CODEC(DoubleDelta)
)
ENGINE = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, mapping_id)
ORDER BY (oid, mapping_id, location_hash);


CREATE TABLE IF NOT EXISTS parca.metadata_stacktraces
(
    `oid` String DEFAULT '0',
    `mapping_id` String,
    `location_hash` String,
    `location_ids_hash` String,
    `payload` String,
    `timestamp_ms` Int64 CODEC(DoubleDelta)
)
ENGINE = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, mapping_id)
ORDER BY (oid, mapping_id, location_hash, location_ids_hash);


CREATE TABLE IF NOT EXISTS parca.profiles_input
(
    `oid` String DEFAULT '0',
    `timestamp_ms` Int64 CODEC(DoubleDelta),
    `profile_type` String,
    `profile_id` UUID,
    `name` String,
    `sample_type` String,
    `sample_unit` String,
    `period_type` String,
    `period_unit` String,
    `period` Int64,
    `duration` Bool,
    `labels` Array(Tuple(String, String)),
    `samples` Array(Tuple(String, Int64)),
    `value` Int64
)
ENGINE = Null;

CREATE TABLE IF NOT EXISTS parca.profiles
(
    `oid` String DEFAULT '0',
    `timestamp_ms` Int64 CODEC(DoubleDelta),
    `profile_type` String,
    `profile_id` UUID,
    `period` Int64,
    `labels` Array(Tuple(String, String)),
    `value` Int64
)
ENGINE = MergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))), profile_type) 
ORDER BY (oid, timestamp_ms, profile_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS parca.profiles_mv TO parca.profiles AS
SELECT
    oid,
    timestamp_ms,
    profile_type,
    profile_id,
    labels,
    value
FROM parca.profiles_input;

CREATE TABLE IF NOT EXISTS parca.profile_types
(
    `oid` String DEFAULT '0',
    `date` Date,
    `name` String,
    `sample_type` String,
    `sample_unit` String,
    `period_type` String,
    `period_unit` String,
    `delta` Bool
) Engine = ReplacingMergeTree() TTL date + INTERVAL 7 DAY
PARTITION BY (date)
ORDER BY (oid, date, name, sample_type, sample_unit, period_type, period_unit, delta);

CREATE MATERIALIZED VIEW IF NOT EXISTS parca.profile_types_mv TO parca.profile_types AS
SELECT
    oid,
    toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))) as date,
    name,
    sample_type,
    sample_unit,
    period_type,
    period_unit,
    CAST(duration > 0, 'Bool') AS delta
FROM parca.profiles_input;


CREATE TABLE IF NOT EXISTS parca.profile_ref_labels
(
    `oid` String DEFAULT '0',
    `timestamp_ms` Int64 CODEC(DoubleDelta),
    `profile_type` String,
    `profile_id` UUID,
    `key` String,
    `value` String
)
Engine = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))), profile_type)
ORDER BY (oid, timestamp_ms, profile_type, key, value, profile_id);


-- <name>:<sample-type>:<sample-unit>:<period-type>:<period-unit>(:delta),
CREATE MATERIALIZED VIEW IF NOT EXISTS parca.profile_ref_labels_mv TO parca.profile_ref_labels AS
SELECT
    oid,
    timestamp_ms,
    profile_type,
    profile_id,
    labels.1 AS key,
    labels.2 AS value
FROM parca.profiles_input
ARRAY JOIN labels;

CREATE TABLE IF NOT EXISTS parca.profile_labels
(
    `oid` String DEFAULT '0',
    `date` Date,
    `key` String,
    `value_id` UInt64,
    `value` String
)
ENGINE = ReplacingMergeTree() TTL date + INTERVAL 7 DAY
PARTITION BY date
ORDER BY (oid, date, key, value_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS parca.profile_labels_mv TO parca.profile_labels AS
SELECT
    oid,
    toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))) as date,
    key,
    cityHash64(value) % 10000 AS value_id,
    value
FROM parca.profile_ref_labels;

CREATE TABLE IF NOT EXISTS parca.profile_stacktraces
(
    `oid` String DEFAULT '0',
    `timestamp_ms` Int64 CODEC(DoubleDelta),
    `profile_type` String,
    `profile_id` UUID,
    `value` Int64,
    `stacktrace_id` String
)
ENGINE = ReplacingMergeTree() TTL toDateTime(timestamp_ms / 1000) + INTERVAL 7 DAY
PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ms, 1000))), profile_type)
ORDER BY (oid, timestamp_ms, profile_type, profile_id, value, stacktrace_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS parca.profile_stacktraces_mv TO parca.profile_stacktraces AS
SELECT
    oid,
    timestamp_ms,
    profile_type,
    profile_id,
    samples.2 AS value,
    samples.1 AS stacktrace_id
FROM parca.profiles_input
ARRAY JOIN samples;

