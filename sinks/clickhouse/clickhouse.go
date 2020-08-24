package clickhouse

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"
	"gitlab.com/chenziliang/pkg-go/utils"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type clickHouse struct {
	config *models.Config
	logger *zap.Logger

	devLocations map[string][]models.LatLon

	db *sqlx.DB
}

func init() {
	sinks.RegisterSink("clickhouse", NewClickHouse)
}

func NewClickHouse(config *models.Config, logger *zap.Logger) (sinks.Sink, error) {
	url, err := getConnectionURL(&config.Sink)
	if err != nil {
		return nil, err
	}

	db, err := sqlx.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}

	return &clickHouse{
		config: config,
		logger: logger,
		db:     db,
	}, nil
}

func getConnectionURL(config *models.ServerConfig) (string, error) {
	url := fmt.Sprintf("tcp://%s", config.Addresses[0])

	var params []string

	altHosts := strings.Join(config.Addresses[1:], ",")
	if len(altHosts) > 0 {
		params = append(params, altHosts)
	}

	if config.Cred.Ctx != nil {
		cred, ok := config.Cred.Ctx.(map[interface{}]interface{})
		if ok {
			for k, v := range cred {
				kk, okk := k.(string)
				vv, okv := v.(string)
				if okk && okv {
					params = append(params, fmt.Sprintf("%s=%s", kk, vv))
				}
			}
		}
	}

	if len(params) > 0 {
		return fmt.Sprintf("%s?%s", url, strings.Join(params, "&")), nil
	}
	return url, nil
}

func (ch *clickHouse) LoadData() {
	var wg sync.WaitGroup
	for _, typ := range ch.config.Settings.Types {
		ch.loadDataFor(typ, &wg)
	}
	wg.Wait()
}

func (ch *clickHouse) loadDataFor(typ string, wg *sync.WaitGroup) {
	if typ == models.METRIC {
		ch.loadMetricData(wg)
	} else if typ == models.LOG {
		ch.loadLogData(wg)
	}
}

func (ch *clickHouse) loadMetricData(wg *sync.WaitGroup) {
	if err := ch.newTimeSeriesTable(); err != nil {
		return
	}

	ch.generateDeviceLocations()

	for i := 0; i < int(ch.config.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadMetricData(i, wg)
	}
}

func (ch *clickHouse) doLoadMetricData(i int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		records := models.GenerateTimeSeriesRecords(ch.config.Settings.TotalEntities, ch.devLocations)
		start := time.Now().UnixNano()
		for n := 0; n < len(records); n += int(ch.config.Settings.BatchSize) {
			pos := n + int(ch.config.Settings.BatchSize)
			if pos > len(records) {
				pos = len(records)
			}

			ch.doMetricInsert(records[n:pos])
		}
		ch.logger.Info("data insert cost", zap.Int64("time", time.Now().UnixNano()-start), zap.Int("total_records", len(records)))
		time.Sleep(time.Duration(ch.config.Settings.Interval) * time.Second)
	}
}

func (ch *clickHouse) doMetricInsert(records []models.Metric) error {
	query := "INSERT INTO default.devices (devicename, region, version, lat, lon, battery, humidity, temperature, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for _, record := range records {
				_, err := stmt.Exec(
					record.Devicename,
					record.Region,
					record.Version,
					record.Lat,
					record.Lon,
					record.Battery,
					record.Humidity,
					record.Temperature,
					record.Timestamp,
				)
				if err != nil {
					ch.logger.Error("failed to insert records", zap.Error(err))
					return 0, err
				}
			}

			return len(records), nil
		},
		query,
	)
}

func (ch *clickHouse) generateDeviceLocations() {
	dbFile := ch.config.Settings.LastRunStateDB
	if utils.FileExists(dbFile) {
		if ch.loadDeviceLocations() == nil {
			return
		}
	}

	ch.devLocations = models.GenerateDeviceLocations(ch.config.Settings.TotalEntities)

	ch.dumpDeviceLocations()
}

func (ch *clickHouse) dumpDeviceLocations() error {
	// save the location information for next use
	dbFile := ch.config.Settings.LastRunStateDB
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

func (ch *clickHouse) loadDeviceLocations() error {
	dbFile := ch.config.Settings.LastRunStateDB
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

func (ch *clickHouse) loadLogData(wg *sync.WaitGroup) {
	if err := ch.newLogTable(); err != nil {
		return
	}

	for i := 0; i < int(ch.config.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadLogData(i, wg)
	}
}

func (ch *clickHouse) doLoadLogData(i int, wg *sync.WaitGroup) {
	defer wg.Done()

	results := make(chan *models.Log, ch.config.Settings.BatchSize)
	errChan := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()

		// 2020.08.14 06:14:04.397465
		tsRegex := `^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}\.\d*`
		err := models.GenerateLogRecords(ch.config.Settings.SampleLogFile, tsRegex, tsRegex, "clickhouse", results)
		if err != nil {
			ch.logger.Error("failed to generate log records", zap.Error(err))
			errChan <- err
		}
	}()

	var batches []*models.Log
	for {
		select {
		case record := <-results:
			if record == nil {
				ch.doLogInsert(batches)
				ch.logger.Info("done with log data loading")
				return
			}

			batches = append(batches, record)
			if len(batches) >= int(ch.config.Settings.BatchSize) {
				ch.doLogInsert(batches)
				batches = nil
			}

		case <-errChan:
			return
		}
	}
}

func (ch *clickHouse) doLogInsert(records []*models.Log) error {
	query := "INSERT INTO default.logs (data, type, _index_time) VALUES (?, ? ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for _, record := range records {
				_, err := stmt.Exec(
					record.Data,
					record.Type,
					record.IndexTime,
				)

				if err != nil {
					ch.logger.Error("failed to insert records", zap.Error(err))
					return 0, err
				}
			}
			return len(records), nil
		},
		query,
	)
}

func (ch *clickHouse) doInsert(prepareFunc func(*sql.Stmt) (int, error), query string) error {
	tx, err := ch.db.Begin()
	if err != nil {
		ch.logger.Error("failed to begin batch", zap.Error(err))
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		ch.logger.Error("failed to prepare insert statement", zap.Error(err))
		return err
	}
	defer stmt.Close()

	n, err := prepareFunc(stmt)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		ch.logger.Error("failed to commit records", zap.Error(err))
	} else {
		ch.logger.Info("inserted records", zap.Int("records", n))
	}

	return err
}

func (ch *clickHouse) Stop() {
	// TODO, graceful teardown
}

func (ch *clickHouse) newTimeSeriesTable() error {
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

func (ch *clickHouse) newLogTable() error {
	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.logs (
			data String,
			type String,
			_index_time DateTime
		) ENGINE MergeTree()
		ORDER BY type
		PARTITION BY (type, toYYYYMMDD(_index_time))
	`)
	if err != nil {
		ch.logger.Error("failed to create source logs table", zap.Error(err))
		return err
	}

	_, err = ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.chlogs (
			_index_time DateTime Codec(DoubleDelta, ZSTD),
			_time DateTime Codec(DoubleDelta, ZSTD),
			thread Int32 Codec(ZSTD),
			level LowCardinality(FixedString(16)),
			message String,
			INDEX levelidx (level) TYPE set(10) GRANULARITY 4,
			INDEX msgidx (message) TYPE tokenbf_v1(1048576, 2, 133) GRANULARITY 8192
		) ENGINE MergeTree()
		ORDER BY _time
		PARTITION BY toYYYYMMDD(_time)
	`)
	if err != nil {
		ch.logger.Error("failed to create sink logs table", zap.Error(err))
		return err
	}

	_, err = ch.db.Exec(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS default.chlogs_v TO chlogs AS 
		SELECT 
		    _index_time,
		    parseDateTimeBestEffortOrZero(replaceRegexpOne(extract(data, '(?P<timestamp>^[\\d|\\.]+ [\\d|\\.|:]+)'), '(\d{4})\.(\d{2})\.(\d{2})', '\\1-\\2-\\3')) AS _time,
		    toInt32OrZero(extract(data, '\[\s+(?P<thread>\d+)\s+\]')) AS thread,
		    extract(data, '\{\}\s+<(?P<level>\w+)+>') AS level,
		    extract(data, '\{\}\s+<\w+>\s+(?P<message>.+)') AS message
	    FROM logs WHERE type='clickhouse'
	`)

	if err != nil {
		ch.logger.Error("failed to create materialized view", zap.Error(err))
	}

	return err
}
