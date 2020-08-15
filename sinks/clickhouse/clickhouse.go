package clickhouse

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type clickHouse struct {
	config *models.Config
	logger *zap.Logger

	devLocations map[string][]latLon

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
	if err := newTimeSeriesTable(ch.db); err != nil {
		ch.logger.Error("failedto create table", zap.Error(err))
		return
	}

	ch.generateDeviceLocations()

	var wg sync.WaitGroup

	for i := 0; i < int(ch.config.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadData(i, &wg)
	}

	wg.Wait()

	/*var rows []struct {
		Database  string `db:"database"`
		Name      string `db:"name"`
		Temporary bool   `db:"is_temporary"`
	}

	if err := ch.db.Select(&rows, "SELECT database, name, is_temporary FROM system.tables"); err != nil {
		ch.logger.Error("failed to query system.tables", zap.Error(err))
	}

	for _, row := range rows {
		ch.logger.Info("row", zap.String("database", row.Database), zap.String("name", row.Name), zap.Bool("is_temporary", row.Temporary))
	}*/
}

func (ch *clickHouse) doLoadData(i int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		records := generateTimeSeriesRecords(ch.config.Settings.TotalEntities, ch.devLocations)
		start := time.Now().UnixNano()
		for n := 0; n < len(records); n += int(ch.config.Settings.BatchSize) {
			pos := n + int(ch.config.Settings.BatchSize)
			if pos > len(records) {
				pos = len(records)
			}

			ch.doInsert(records[n:pos])
		}
		ch.logger.Info("data insert cost", zap.Int64("time", time.Now().UnixNano()-start), zap.Int("total_records", len(records)))
		time.Sleep(time.Duration(ch.config.Settings.Interval) * time.Second)
	}
}

func (ch *clickHouse) doInsert(records []dataPoint) error {
	tx, err := ch.db.Begin()
	if err != nil {
		ch.logger.Error("failed to begin batch", zap.Error(err))
		return err
	}

	insert := "INSERT INTO default.devices (devicename, region, version, lat, lon, battery, humidity, temperature, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?"
	stmt, err := tx.Prepare(insert)
	if err != nil {
		ch.logger.Error("failed to prepare insert statement", zap.Error(err))
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.Exec(
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
			continue
		}
	}

	if err = tx.Commit(); err != nil {
		ch.logger.Error("failed to commit records", zap.Error(err))
	}

	ch.logger.Info("inserted records", zap.Int("records", len(records)))
	return err
}

func (ch *clickHouse) generateDeviceLocations() {
	ch.logger.Info("start generating locations")
	ch.devLocations = generateDeviceLocations(ch.config.Settings.TotalEntities)
	ch.logger.Info("finished generating locations")
}

func (ch *clickHouse) Stop() {
	// TODO, graceful teardown
}
