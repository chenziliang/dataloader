package clickhouse

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/pkg-go/utils"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if err := ch.newTimeSeriesTable(source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	ch.generateDeviceLocations(source)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadMetricData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	for {
		records := models.GenerateTimeSeriesRecords(source.Settings.TotalEntities, ch.devLocations)
		start := time.Now().UnixNano()
		for n := 0; n < len(records); n += int(source.Settings.BatchSize) {
			pos := n + int(source.Settings.BatchSize)
			if pos > len(records) {
				pos = len(records)
			}

			ch.doMetricInsert(records[n:pos], source.Type)
		}
		ch.logger.Info("data insert cost", zap.Int64("time", time.Now().UnixNano()-start), zap.Int("total_records", len(records)))
		time.Sleep(time.Duration(source.Settings.Interval) * time.Second)
	}
}

func (ch *clickHouse) doMetricInsert(records []models.Metric, typ string) error {
	query := "INSERT INTO default.devices (devicename, region, version, lat, lon, battery, humidity, temperature, hydraulic_pressure, atmospheric_pressure, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].Devicename,
					records[i].Region,
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

func (ch *clickHouse) generateDeviceLocations(source *models.Source) {
	dbFile := source.Settings.LastRunStateDB
	if utils.FileExists(dbFile) {
		if ch.loadDeviceLocations(dbFile) == nil {
			return
		}
	}

	ch.devLocations = models.GenerateDeviceLocations(source.Settings.TotalEntities)

	ch.dumpDeviceLocations(dbFile)
}

func (ch *clickHouse) dumpDeviceLocations(dbFile string) error {
	// save the location information for next use
	data, err := json.Marshal(&ch.devLocations)
	if err != nil {
		ch.logger.Info("failed to marshal device location info", zap.Error(err))
		return err
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		ch.logger.Error("failed to compress device location data", zap.Error(err))
		return err
	}
	zw.Close()

	err = ioutil.WriteFile(dbFile, buf.Bytes(), 0644)
	if err != nil {
		ch.logger.Error("failed to persitent device location information in db", zap.Error(err))
	} else {
		ch.logger.Info("persitented device location information in db")
	}
	return err
}

func (ch *clickHouse) loadDeviceLocations(dbFile string) error {
	ch.logger.Info("loading device location", zap.String("device_location_db", dbFile))

	data, err := ioutil.ReadFile(dbFile)
	if err != nil {
		ch.logger.Error("failed to read device location db file", zap.Error(err))
		return err
	}

	var buf bytes.Buffer
	buf.Write(data)

	zr, err := gzip.NewReader(&buf)
	if err != nil {
		ch.logger.Error("failed to create gzip reader", zap.Error(err))
		return err
	}
	defer zr.Close()

	data, err = ioutil.ReadAll(zr)
	if err != nil {
		ch.logger.Error("failed to decompress data", zap.Error(err))
		return err
	}

	err = json.Unmarshal(data, &ch.devLocations)
	if err != nil {
		ch.logger.Error("failed to load device location db", zap.Error(err))
	} else {
		ch.logger.Info("successfully load device location db")
	}
	return err
}

func (ch *clickHouse) newTimeSeriesTable(cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.devices`); err != nil {
			ch.logger.Error("failed to drop devices table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped devices table")
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.devices (
			devicename String,
			region LowCardinality(String),
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
		ORDER BY (devicename, timestamp)
		PARTITION BY (region, toYYYYMM(timestamp))
	`)
	if err != nil {
		ch.logger.Error("failed to create metric table", zap.Error(err))
	}
	return err
}
