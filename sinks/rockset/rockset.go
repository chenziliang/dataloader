package rocks

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/openapi"
	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"
	"go.uber.org/zap"
)

type rockSet struct {
	config *models.Config
	logger *zap.Logger

	devLocations map[string][]models.LatLon

	lock sync.Mutex

	ingested_total uint64
	duration_total uint64

	ingested uint64
	duration uint64

	workspace string

	client *rockset.RockClient
}

func init() {
	sinks.RegisterSink("rockset", NewRockSet)
}

func NewRockSet(config *models.Config, logger *zap.Logger) (sinks.Sink, error) {
	var key string

	if config.Sink.Cred.Ctx != nil {
		if config.Sink.Cred.Type != "api_key" {
			return nil, errors.New("only support api key credential for now")
		}

		cred, ok := config.Sink.Cred.Ctx.(map[interface{}]interface{})
		if ok {
			if cred["key"] != nil {
				k, ok := cred["key"].(string)
				if ok {
					key = k
				} else {
					return nil, errors.New("api key is not string")
				}
			} else {
				return nil, errors.New("`key` is missing")
			}
		} else {
			return nil, errors.New("Missing api key")
		}
	}

	if len(config.Sink.Addresses) == 0 {
		return nil, errors.New("Missing server")
	}

	rc, err := rockset.NewClient(rockset.WithAPIKey(key), rockset.WithAPIServer(config.Sink.Addresses[0]))
	if err != nil {
		return nil, err
	}

	return &rockSet{
		config:    config,
		logger:    logger,
		workspace: "commons",
		client:    rc,
	}, nil
}

func (rc *rockSet) LoadData() {
	var wg sync.WaitGroup
	for i := range rc.config.Sources {
		if rc.config.Sources[i].Enabled {
			rc.loadDataFor(&rc.config.Sources[i], &wg)
		}
	}
	wg.Wait()
}

func (rc *rockSet) loadDataFor(source *models.Source, wg *sync.WaitGroup) {
	if source.Type == models.METRIC {
		rc.loadMetricData(source, wg)
	} else if source.Type == models.LOG {
		rc.logger.Error("unsupported data type", zap.String("type", source.Type))
	}
}

func (rc *rockSet) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if source.Settings.Table == "" {
		source.Settings.Table = "device_metrics"
	}

	rc.logger.Info("target collection", zap.String("collection", source.Settings.Table))

	if err := rc.newCollection(source.Settings.Table, source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	rc.devLocations = sinks.GenerateLocations(source, true)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go rc.doLoadMetricData(source, wg, i)
	}
}

func (rc *rockSet) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32
	batchSize := int(source.Settings.BatchSize)

	start := time.Now().UnixNano()
	prev := start

	// records := models.GenerateMetrics(source.Settings.TotalEntities, rc.devLocations)
	for {
		records := models.GenerateMetrics(source.Settings.TotalEntities, rc.devLocations)

		atomic.AddUint64(&rc.ingested, (uint64)(len(records)))
		atomic.AddUint64(&rc.ingested_total, (uint64)(len(records)))

		batch := records
		for n := 0; n < len(batch); n += batchSize {
			pos := n + batchSize
			if pos > len(batch) {
				pos = len(batch)
			}

			/// now := time.Now().UnixNano()
			rc.doMetricInsert(batch[n:pos], source.Settings.Table, source.Type)
			/// atomic.AddUint64(&ch.duration, uint64(time.Now().UnixNano()-now))
			/// atomic.AddUint64(&ch.duration_total, uint64(time.Now().UnixNano()-now))

			currentIteration += 1
		}

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			current_duration_ms := uint64((now - prev) / 1000000)
			current_ingested := atomic.LoadUint64(&rc.ingested)

			ingested_total := atomic.LoadUint64(&rc.ingested_total)
			duration_total_ms := uint64((now - start) / 1000000)

			/// reset to 0
			atomic.StoreUint64(&rc.ingested, 0)

			rc.logger.Info("ingest metrics", zap.Uint64("ingested", current_ingested), zap.Uint64("duration_ms", current_duration_ms), zap.Uint64("eps", (current_ingested*1000)/current_duration_ms), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total_ms), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total_ms))

			prev = now
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (rc *rockSet) doMetricInsert(records []models.Metric, collection, typ string) error {
	now := time.Now()
	ctx := context.TODO()
	var docs []interface{}

	for i := range records {
		docs = append(docs, map[string]interface{}{
			"device":               records[i].Devicename,
			"region":               records[i].Region,
			"city":                 records[i].City,
			"version":              records[i].Version,
			"lat":                  records[i].Lat,
			"lon":                  records[i].Lon,
			"battery":              records[i].Battery,
			"humidity":             records[i].Humidity,
			"temperature":          records[i].Temperature,
			"hydraulic_pressure":   records[i].HydraulicPressure,
			"atmospheric_pressure": records[i].AtmosphericPressure,
			"timestamp":            now,
		})
	}

	statuses, err := rc.client.AddDocuments(ctx, rc.workspace, collection, docs)
	if err != nil {
		return err
	}

	for _, status := range statuses {
		if status.Error != nil {
			return errors.New("failed to ingest document " + status.Error.GetMessage())
		}
	}

	return nil
}

func (rc *rockSet) newCollection(collection string, cleanBeforeLoad bool) error {
	ctx := context.TODO()

	if cleanBeforeLoad {
		if err := rc.client.DeleteCollection(ctx, rc.workspace, collection); err != nil {
			rc.logger.Error("failed to drop collection", zap.String("collection", collection), zap.Error(err))
			return err
		}

		if err := rc.client.WaitUntilCollectionGone(ctx, rc.workspace, collection); err != nil {
			rc.logger.Error("failed to wait collection gone after dropped it", zap.String("collection", collection), zap.Error(err))
			return err
		}

		rc.logger.Info("dropped collection successfully", zap.String("collection", collection))
	}

	if _, err := rc.client.GetCollection(ctx, rc.workspace, collection); err == nil {
		rc.logger.Info("collection exists", zap.String("collection", collection))
		return nil
	}

	var desc string = "perf testing"
	var rention_seconds int64 = 86400 * 100
	var insert_only bool = false
	var partition_key1 string = "city"
	var partition_key2 string = "device"
	var partition_key_type string = "AUTO"

	req := openapi.NewCreateCollectionRequest(collection)
	req.Description = &desc
	req.RetentionSecs = &rention_seconds
	req.InsertOnly = &insert_only
	req.EventTimeInfo = openapi.NewEventTimeInfo("timestamp")
	req.ClusteringKey = []openapi.FieldPartition{openapi.FieldPartition{&partition_key1, &partition_key_type, nil}, openapi.FieldPartition{&partition_key2, &partition_key_type, nil}}

	if _, err := rc.client.CreateCollection(ctx, rc.workspace, collection, req); err != nil {
		rc.logger.Error("failed to create collection", zap.String("collection", collection), zap.Error(err))
		return err
	}

	if err := rc.client.WaitUntilCollectionReady(ctx, rc.workspace, collection); err != nil {
		rc.logger.Error("failed to wait collection ready after creating it", zap.String("collection", collection), zap.Error(err))
		return err
	}
	return nil
}

func (rc *rockSet) Stop() {
}
