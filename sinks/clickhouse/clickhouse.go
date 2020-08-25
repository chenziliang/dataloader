package clickhouse

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"

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
	for i := range ch.config.Sources {
		if ch.config.Sources[i].Enabled {
			ch.loadDataFor(&ch.config.Sources[i], &wg)
		}
	}
	wg.Wait()
}

func (ch *clickHouse) loadDataFor(source *models.Source, wg *sync.WaitGroup) {
	if source.Type == models.METRIC {
		ch.loadMetricData(source, wg)
	} else if source.Type == models.LOG {
		ch.loadLogData(source, wg)
	} else if source.Type == models.CRIME_CASE {
		ch.loadCrimeData(source, wg)
	}
}

func (ch *clickHouse) doInsert(prepareFunc func(*sql.Stmt) (int, error), query string, typ string) error {
	tx, err := ch.db.Begin()
	if err != nil {
		ch.logger.Error("failed to begin batch", zap.String("type", typ), zap.Error(err))
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		ch.logger.Error("failed to prepare insert statement", zap.String("type", typ), zap.Error(err))
		return err
	}
	defer stmt.Close()

	n, err := prepareFunc(stmt)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		ch.logger.Error("failed to commit records", zap.String("type", typ), zap.Error(err))
	} else {
		ch.logger.Info("inserted records", zap.String("type", typ), zap.Int("records", n))
	}

	return err
}

func (ch *clickHouse) Stop() {
	// TODO, graceful teardown
}
