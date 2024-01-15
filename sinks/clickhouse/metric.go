package clickhouse

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"
)

func (ch *clickHouse) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if source.Settings.Table == "" {
		source.Settings.Table = "default.device_metrics"
	}

	ch.logger.Info("target table", zap.String("table", source.Settings.Table))

	if err := ch.newDeviceTable(source.Settings.Table, source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	ch.devLocations = sinks.GenerateLocations(source, true)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadMetricData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32
	batchSize := int(source.Settings.BatchSize)

	start := time.Now().UnixNano()
	prev := start

	// records := models.GenerateMetrics(source.Settings.TotalEntities, ch.devLocations)
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

			/// now := time.Now().UnixNano()
			ch.doMetricInsert(batch[n:pos], source.Settings.Table, source.Type)
			/// atomic.AddUint64(&ch.duration, uint64(time.Now().UnixNano()-now))
			/// atomic.AddUint64(&ch.duration_total, uint64(time.Now().UnixNano()-now))

			currentIteration += 1
		}

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			current_duration_ms := uint64((now - prev) / 1000000)
			current_ingested := atomic.LoadUint64(&ch.ingested)

			ingested_total := atomic.LoadUint64(&ch.ingested_total)
			duration_total_ms := uint64((now - start) / 1000000)

			/// reset to 0
			atomic.StoreUint64(&ch.ingested, 0)

			ch.logger.Info("ingest metrics", zap.Uint64("ingested", current_ingested), zap.Uint64("duration_ms", current_duration_ms), zap.Uint64("eps", (current_ingested*1000)/current_duration_ms), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total_ms), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total_ms))

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

func (ch *clickHouse) doMetricInsert(records []models.Metric, table, typ string) error {
	now := time.Now()

	query := "INSERT INTO " + table + " (device, region, city, version, lat, lon, battery, humidity, temperature, hydraulic_pressure, atmospheric_pressure, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

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
					now,
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

func (ch *clickHouse) newDeviceTable(table string, cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP STREAM IF EXISTS ` + table); err != nil {
			ch.logger.Error("failed to drop device metrics table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped devices table")
	}

	if _, err := ch.db.Exec(`DESCRIBE STREAM ` + table); err == nil {
		ch.logger.Info("devices table exists")
		return nil
	}

	_, err := ch.db.Exec(`
		CREATE STREAM IF NOT EXISTS ` + table + ` (
			device string,
			region string,
			city string,
			version string,
			lat float32 CODEC(Gorilla, LZ4HC(9)),
			lon float32 CODEC(Gorilla, LZ4HC(9)),
			battery float32 CODEC(Gorilla, LZ4HC),
			humidity uint16 CODEC(Delta(2), LZ4HC),
			temperature int16 CODEC(Delta(2), LZ4HC),
			hydraulic_pressure float32 CODEC(Delta(2), LZ4HC),
			atmospheric_pressure float32 CODEC(Delta(2), LZ4HC),
			timestamp datetime64(3)
		)
	`)
	if err != nil {
		ch.logger.Error("failed to create devices metric table", zap.Error(err))
	}
	return err
}
