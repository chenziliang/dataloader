package clickhouse

import (
	"database/sql"
	"sync"
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
	var batch []models.Metric
	batchSize := int(source.Settings.BatchSize)

	for {
		records := models.GenerateMetrics(source.Settings.TotalEntities, ch.devLocations)
		batch = append(batch, records...)

		if len(batch) >= int(source.Settings.BatchSize) {
			for n := 0; n < len(batch); n += batchSize {
				pos := n + batchSize
				if pos > len(batch) {
					pos = len(batch)
				}

				ch.doMetricInsert(batch[n:pos], source.Type)
				currentIteration += 1
			}
			batch = batch[:0]
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (ch *clickHouse) doMetricInsert(records []models.Metric, typ string) error {
	query := "INSERT INTO default.device_metrics (devicename, region, city, version, lat, lon, battery, humidity, temperature, hydraulic_pressure, atmospheric_pressure, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

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
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.device_metrics`); err != nil {
			ch.logger.Error("failed to drop device metrics table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped devices table")
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.device_metrics (
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
			timestamp DateTime Codec(DoubleDelta, ZSTD) 
		) ENGINE = MergeTree()
		ORDER BY (region, city, toYYYYMMDD(timestamp), devicename)
		PARTITION BY (region, city, toYYYYMMDD(timestamp))
	`)
	if err != nil {
		ch.logger.Error("failed to create devices metric table", zap.Error(err))
	}
	return err
}
