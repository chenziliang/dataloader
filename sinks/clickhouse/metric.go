package clickhouse

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if err := ch.newDeviceTable(source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	ch.devLocations = ch.generateLocations(source, true)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadMetricData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32
	batchSize := int(source.Settings.BatchSize)

	table := "default.metrics"
	if source.Settings.Table != "" {
		table = source.Settings.Table
	}
	ch.logger.Info("target table", zap.String("table", table))

	start := time.Now().UnixNano()
	prev := start

	for {
		records := models.GenerateMetrics(source.Settings.TotalEntities, ch.devLocations)

		atomic.AddUint64(&ch.ingested, (uint64)(len(records)))
		atomic.AddUint64(&ch.ingested_total, (uint64)(len(records)))

		batch := records
		for n := 0; n < len(batch); n += batchSize {
			pos := n + batchSize
			if pos > len(batch) {
				pos = len(batch)
			}

			now := time.Now().UnixNano()
			ch.doMetricInsert(batch[n:pos], table, source.Type)
			atomic.AddUint64(&ch.duration, uint64(time.Now().UnixNano()-now))
			atomic.AddUint64(&ch.duration_total, uint64(time.Now().UnixNano()-now))

			currentIteration += 1
		}

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			/// every 5 seconds
			prev = now

			ingested_total := atomic.LoadUint64(&ch.ingested_total)
			duration_total := atomic.LoadUint64(&ch.duration_total) / 1000000 / uint64(source.Settings.Concurrency)

			ingested := atomic.LoadUint64(&ch.ingested)
			duration := atomic.LoadUint64(&ch.duration) / 1000000 / uint64(source.Settings.Concurrency)

			/// reset to 0
			atomic.StoreUint64(&ch.ingested, 0)
			atomic.StoreUint64(&ch.duration, 0)

			if duration == 0 {
				ch.logger.Warn("Zero duration ???", zap.Uint64("ingested", ingested))
				continue
			}

			ch.logger.Info("ingest metrics", zap.Uint64("ingested", ingested), zap.Uint64("duration_ms", duration), zap.Uint64("eps", (ingested*1000)/duration), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total))
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (ch *clickHouse) doMetricInsert(records []models.Metric, table, typ string) error {
	query := "INSERT INTO " + table + " (devicename, region, city, version, lat, lon, battery, humidity, temperature, hydraulic_pressure, atmospheric_pressure, _time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].Devicename,
					records[i].Region,
					records[i].City,
					records[i].Version,
					records[i].Lat,
					records[i].Lon,
					records[i].Battery,
					records[i].Humidity,
					records[i].Temperature,
					records[i].HydraulicPressure,
					records[i].AtmosphericPressure,
					records[i].Timestamp,
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

func (ch *clickHouse) newDeviceTable(cleanBeforeLoad bool) error {
	return nil
	/*if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.device_metrics`); err != nil {
			ch.logger.Error("failed to drop device metrics table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped devices table")
	}

	if _, err := ch.db.Exec(`DESCRIBE TABLE default.device_metrics`); err == nil {
		ch.logger.Info("devices table exists")
		return nil
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.metrics (
			devicename String,
			region LowCardinality(String),
			city LowCardinality(String),
			version LowCardinality(String),
			lat Float32 CODEC(Gorilla, LZ4HC(9)),
			lon Float32 CODEC(Gorilla, LZ4HC(9)),
			battery Float32 CODEC(Gorilla, LZ4HC),
			humidity UInt16 CODEC(Delta(2), LZ4HC),
			temperature Int16 CODEC(Delta(2), LZ4HC),
			hydraulic_pressure Float32 CODEC(Delta(2), LZ4HC),
			atmospheric_pressure Float32 CODEC(Delta(2), LZ4HC),
			_time DateTime64(3) DEFAULT now64(3), Codec(DoubleDelta, ZSTD)
		) ENGINE = DistributedMergeTree(1, 3, rand(6yui87^YUI*&))
		ORDER BY (region, city, toYYYYMMDD(timestamp), devicename)
		PARTITION BY (region, city, toYYYYMMDD(timestamp))
	`)
	if err != nil {
		ch.logger.Error("failed to create devices metric table", zap.Error(err))
	}
	return err*/
}
