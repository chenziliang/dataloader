package clickhouse

import (
	"database/sql"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadUnstructureData(source *models.Source, wg *sync.WaitGroup) {
	ch.lock.Lock()
	if ch.unstructureDevLocations == nil {
		if err := ch.newUnstructureDeviceTable(source.Settings.CleanBeforeLoad); err != nil {
			ch.lock.Unlock()
			return
		}

		ch.unstructureDevLocations = ch.generateLocations(source, true)
	}
	ch.lock.Unlock()

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadUnstructureMetricData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadUnstructureMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	deadline := time.Now().Add(time.Second * time.Duration(source.Settings.Duration))
	lastRun := make(map[string]float32)

	for {
		records := models.GenerateUnstructureMetrics(source, ch.unstructureDevLocations, uint32(deadline.Unix()), lastRun)
		start := time.Now().UnixNano()
		for n := 0; n < len(records); n += int(source.Settings.BatchSize) {
			pos := n + int(source.Settings.BatchSize)
			if pos > len(records) {
				pos = len(records)
			}

			ch.doUnstructureMetricInsert(records[n:pos], source.Type)
		}
		ch.logger.Info("data insert cost", zap.Int64("time", time.Now().UnixNano()-start), zap.Int("total_records", len(records)))
		time.Sleep(time.Duration(source.Settings.Interval) * time.Second)
	}
}

func (ch *clickHouse) doUnstructureMetricInsert(records []models.UnstructureMetric, typ string) error {
	query := "INSERT INTO default.unstructure_device_metrics (devicename, region, city, lat, lon, raw, sourcetype, _index_time) VALUES (?, ?, ?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].DeviceName,
					records[i].Region,
					records[i].City,
					records[i].Lat,
					records[i].Lon,
					records[i].Raw,
					records[i].Sourcetype,
					records[i].IndexTime,
				)
				if err != nil {
					ch.logger.Error("failed to insert records", zap.String("type", typ), zap.Error(err))
					return 0, err
				}
			}

			return len(records), nil
		},
		query,
		typ,
	)
}

func (ch *clickHouse) newUnstructureDeviceTable(cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.unstructure_device_metrics`); err != nil {
			ch.logger.Error("failed to drop unstructure device metrics table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped unstructure device metrics table")
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.unstructure_device_metrics (
			devicename FixedString(32),
			region LowCardinality(String),
			city LowCardinality(String),
			lat Float32 CODEC(Gorilla, LZ4HC(9)),
			lon Float32 CODEC(Gorilla, LZ4HC(9)),
			raw String,
			sourcetype LowCardinality(FixedString(16)),
			_index_time DateTime Codec(DoubleDelta, ZSTD) 
		) ENGINE = MergeTree()
		ORDER BY (region, city, toYYYYMMDD(_index_time), devicename)
		PARTITION BY (region, city, toYYYYMMDD(_index_time))
	`)
	if err != nil {
		ch.logger.Error("failed to create unstructure device metrics table", zap.Error(err))
	}
	return err
}
