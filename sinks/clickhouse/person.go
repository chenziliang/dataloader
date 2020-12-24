package clickhouse

import (
	"database/sql"
	"sync"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadPersonData(source *models.Source, wg *sync.WaitGroup) {
	if err := ch.newPersonTable(source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	ch.personLocations = ch.generateLocations(source, false)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadPersonData(source, wg, i)
	}
}

func (ch *clickHouse) doLoadPersonData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	records := models.GeneratePersons(source.Settings.TotalEntities, ch.personLocations)
	start := time.Now().UnixNano()
	for n := 0; n < len(records); n += int(source.Settings.BatchSize) {
		pos := n + int(source.Settings.BatchSize)
		if pos > len(records) {
			pos = len(records)
		}

		ch.doPersonInsert(records[n:pos], source.Type)
	}
	ch.logger.Info("data insert cost", zap.Int64("time", time.Now().UnixNano()-start), zap.Int("total_records", len(records)))
}

func (ch *clickHouse) doPersonInsert(records []models.Person, typ string) error {
	query := "INSERT INTO default.persons (name, id, age, sex, phone, region, city, address, lat, lon, _time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].Name,
					records[i].ID,
					records[i].Age,
					records[i].Sex,
					records[i].Phone,
					records[i].Region,
					records[i].City,
					records[i].Address,
					records[i].Lat,
					records[i].Lon,
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

func (ch *clickHouse) newPersonTable(cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.persons`); err != nil {
			ch.logger.Error("failed to drop persons table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped persons table")
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.persons (
			name String,
			id String,
			age UInt8,
			sex LowCardinality(FixedString(8)),
			phone String,
			region LowCardinality(String),
			city LowCardinality(String),
			address String,
			lat Float32 CODEC(Gorilla, LZ4HC(9)),
			lon Float32 CODEC(Gorilla, LZ4HC(9)),
			_time DateTime64(3) DEFAULT now64(3, 'UTC') Codec(DoubleDelta, ZSTD) 
		) ENGINE = MergeTree()
		ORDER BY (region, city, name)
		PARTITION BY (region, city)
	`)
	if err != nil {
		ch.logger.Error("failed to create perons table", zap.Error(err))
	}
	return err
}
