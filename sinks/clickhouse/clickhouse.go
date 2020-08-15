package clickhouse

import (
	"fmt"
	"strings"

	"github.com/chenziliang/dataloader/models"
	"github.com/chenziliang/dataloader/sinks"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type clickHouse struct {
	config *models.Config
	logger *zap.Logger

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

	connect, err := sqlx.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}

	return &clickHouse{
		config: config,
		logger: logger,
		db:     connect,
	}, nil
}

func getConnectionURL(config *models.ServerConfig) (string, error) {
	url := fmt.Sprintf("tcp://%s", config.Addresses[0])
	if config.Cred.Ctx == nil {
		return url, nil
	}

	var params []string

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

	if len(params) > 0 {
		return fmt.Sprintf("%s?%s", url, strings.Join(params, "&")), nil
	}
	return url, nil
}

func (ch *clickHouse) LoadData() {
	var rows []struct {
		Database  string `db:"database"`
		Name      string `db:"name"`
		Temporary bool   `db:"is_temporary"`
	}

	if err := ch.db.Select(&rows, "SELECT database, name, is_temporary FROM system.tables"); err != nil {
		ch.logger.Error("failed to query system.tables", zap.Error(err))
	}

	for _, row := range rows {
		ch.logger.Info("row", zap.String("database", row.Database), zap.String("name", row.Name), zap.Bool("is_temporary", row.Temporary))
	}
}
