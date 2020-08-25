package clickhouse

import (
	"database/sql"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadLogData(source *models.Source, wg *sync.WaitGroup) {
	if err := ch.newLogTable(source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadLogData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadLogData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	results := make(chan *models.Log, source.Settings.BatchSize)
	errChan := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()

		// 2020.08.14 06:14:04.397465
		tsRegex := `^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}\.\d*`
		err := models.GenerateLogRecords(source.Settings.SampleFile, tsRegex, tsRegex, "clickhouse", results)
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
				ch.doLogInsert(batches, source)
				ch.logger.Info("done with log data loading")
				return
			}

			batches = append(batches, record)
			if len(batches) >= int(source.Settings.BatchSize) {
				ch.doLogInsert(batches, source)
				batches = nil
			}

		case <-errChan:
			return
		}
	}
}

func (ch *clickHouse) doLogInsert(records []*models.Log, source *models.Source) error {
	time.Sleep(time.Duration(source.Settings.Interval) * time.Second)

	query := "INSERT INTO default.logs (data, type, _index_time) VALUES (?, ? ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].Data,
					records[i].Type,
					records[i].IndexTime,
				)

				if err != nil {
					ch.logger.Error("failed to insert records", zap.String("type", source.Type), zap.Error(err))
					return 0, err
				}
			}
			return len(records), nil
		},
		query,
		source.Type,
	)
}

func (ch *clickHouse) newLogTable(cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.logs`); err != nil {
			ch.logger.Error("failed to drop logs table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped logs table")

		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.chlogs`); err != nil {
			ch.logger.Error("failed to drop chlogs table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped chlogs table")

		if _, err := ch.db.Exec(`DROP VIEW IF EXISTS default.chlogs_v`); err != nil {
			ch.logger.Error("failed to drop chlogs_v view", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped chlogs_v view")
	}

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
