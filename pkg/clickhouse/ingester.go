package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/labels"

	metastorepb "github.com/parca-dev/parca/gen/proto/go/parca/metastore/v1alpha1"
	profilestorepb "github.com/parca-dev/parca/gen/proto/go/parca/profilestore/v1alpha1"
	"github.com/parca-dev/parca/pkg/parcacol"
	"github.com/parca-dev/parca/pkg/profile"
)

const (
	defaultWriteBatchDelay time.Duration = 2 * time.Second
	defaultWriteBatchSize  int           = 10000
)

type Ingester struct {
	logger    log.Logger
	metastore metastorepb.MetastoreServiceClient
	db        clickhouse.Conn

	delay  time.Duration
	size   int
	series chan parcacol.Series
	finish chan struct{}
	done   sync.WaitGroup
}

func NewIngester(
	logger log.Logger,
	metastore metastorepb.MetastoreServiceClient,
	db clickhouse.Conn,
) *Ingester {
	ingester := &Ingester{
		logger:    logger,
		metastore: metastore,
		db:        db,
		delay:     defaultWriteBatchDelay,
		size:      defaultWriteBatchSize,
		series:    make(chan parcacol.Series, defaultWriteBatchSize),
		finish:    make(chan struct{}),
	}
	go ingester.backgroundIngest()
	return ingester
}

func (i *Ingester) Close() error {
	close(i.finish)
	i.done.Wait()
	return nil
}

func (i *Ingester) backgroundIngest() {
	batch := make([]parcacol.Series, 0, i.size)
	timer := time.After(i.delay)
	last := time.Now()

	for {
		i.done.Add(1)

		flush := false
		finish := false

		select {
		case series := <-i.series:
			batch = append(batch, series)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(i.delay)
			flush = time.Since(last) > i.delay && len(batch) > 0
		case <-i.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := i.ingest(batch); err != nil {
				level.Error(i.logger).Log("msg", "could not write a batch of series to profile_input table", "err", err)
			}

			batch = make([]parcacol.Series, 0, i.size)
			last = time.Now()
		}

		i.done.Done()

		if finish {
			break
		}
	}
}

func (i *Ingester) ingest(ss []parcacol.Series) error {
	ctx := context.Background()
	profilesBatch, err := i.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", dbName, tableProfilesInput))
	if err != nil {
		return err
	}
	for _, s := range ss {
		for _, normalizedProfiles := range s.Samples {
			for _, p := range normalizedProfiles {
				if len(p.Samples) == 0 {
					ls := labels.FromMap(s.Labels)
					level.Debug(i.logger).Log("msg", "no samples found in profile, dropping it", "name", p.Meta.Name, "sample_type", p.Meta.SampleType.Type, "sample_unit", p.Meta.SampleType.Unit, "labels", ls)
					continue
				}
				if err := i.IngestProfile(ctx, labels.FromMap(s.Labels), p, profilesBatch); err != nil {
					return fmt.Errorf("ingest profile: %w", err)
				}
			}
		}
	}
	if err := profilesBatch.Send(); err != nil {
		return err
	}
	return nil
}

func (i *Ingester) Ingest(ctx context.Context, req *profilestorepb.WriteRawRequest) error {
	normalizedRequest, err := parcacol.NormalizeWriteRawRequest(ctx, parcacol.NewNormalizer(i.metastore), req)
	if err != nil {
		return err
	}
	for _, s := range normalizedRequest.Series {
		i.series <- s
	}
	return nil
}

func buildProfileTypeFromMeta(meta profile.Meta) string {
	tt := []string{meta.Name, meta.SampleType.Type, meta.SampleType.Unit, meta.PeriodType.Type, meta.PeriodType.Unit}
	if meta.Duration > 0 {
		tt = append(tt, "delta")
	}
	return strings.Join(tt, ":")
}

func (i *Ingester) IngestProfile(ctx context.Context, ls labels.Labels, p *profile.NormalizedProfile, profilesBatch driver.Batch) error {
	labels := make([][]string, 0, len(ls))
	for _, label := range ls {
		labels = append(labels, []string{label.Name, label.Value})
	}
	var totalValue int64
	samples := make([]interface{}, 0, len(p.Samples))

	profileID := uuid.NewString()
	for i := range p.Samples {
		sample := p.Samples[i]
		samples = append(samples, []interface{}{sample.StacktraceID, sample.Value})
		totalValue += sample.Value
	}
	if err := profilesBatch.Append(
		"0",
		p.Meta.Timestamp,
		buildProfileTypeFromMeta(p.Meta),
		profileID,
		p.Meta.Name,
		p.Meta.SampleType.Type,
		p.Meta.SampleType.Unit,
		p.Meta.PeriodType.Type,
		p.Meta.PeriodType.Unit,
		p.Meta.Period,
		p.Meta.Duration,
		labels,
		samples,
		totalValue,
	); err != nil {
		return err
	}

	return nil
}
