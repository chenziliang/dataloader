package proton

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"

	_ "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/jmoiron/sqlx"

	"go.uber.org/zap"
)

type proton struct {
	config *models.Config
	logger *zap.Logger

	devLocations            map[string][]models.LatLon
	unstructureDevLocations map[string][]models.LatLon
	personLocations         map[string][]models.LatLon

	lock sync.Mutex

	ingested_total uint64
	duration_total uint64

	ingested uint64
	duration uint64

	db *sqlx.DB
}

func init() {
	sinks.RegisterSink("proton", NewProton)
}

func NewProton(config *models.Config, logger *zap.Logger) (sinks.Sink, error) {
	url, err := getConnectionURL(&config.Sink)
	if err != nil {
		return nil, err
	}

	logger.Info("open url", zap.String("url", url))
	db, err := sqlx.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}

	return &proton{
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
		params = append(params, fmt.Sprintf("alt_hosts=%s", altHosts))
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

	debug := os.Getenv("dataloader_debug")
	if debug != "" {
		params = append(params, fmt.Sprintf("debug=true"))
	}

	if len(params) > 0 {
		return fmt.Sprintf("%s?%s", url, strings.Join(params, "&")), nil
	}
	return url, nil
}

func (ch *proton) LoadData() {
	var wg sync.WaitGroup
	for i := range ch.config.Sources {
		if ch.config.Sources[i].Enabled {
			ch.loadDataFor(&ch.config.Sources[i], &wg)
		}
	}
	wg.Wait()
}

func (ch *proton) loadDataFor(source *models.Source, wg *sync.WaitGroup) {
	if source.Type == models.METRIC {
		ch.loadMetricData(source, wg)
	} else if source.Type == models.LOG {
		ch.loadLogData(source, wg)
	} else if source.Type == models.CRIME_CASE {
		ch.loadCrimeData(source, wg)
	} else if source.Type == models.PERSON {
		ch.loadPersonData(source, wg)
	} else if source.Type == models.UNSTRUCTURE_METRIC {
		ch.loadUnstructureData(source, wg)
	} else {
		ch.logger.Error("unsupported data type", zap.String("type", source.Type))
	}
}

func (ch *proton) doInsert(prepareFunc func(*sql.Stmt) (int, error), query string, typ string) error {
	start := time.Now().UnixNano()

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
		end := time.Now().UnixNano()
		ch.logger.Debug("inserted records", zap.String("type", typ), zap.Int("records", n), zap.Int64("latency", (end-start)/1000))
	}

	return err
}

func (ch *proton) Stop() {
	// TODO, graceful teardown
}
