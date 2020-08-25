package clickhouse

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"io"
	"os"
	"strings"
	"sync"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *clickHouse) loadCrimeData(source *models.Source, wg *sync.WaitGroup) {
	if err := ch.newCrimeTable(source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	ch.doLoadCrimeData(source, wg)
}

func (ch *clickHouse) doLoadCrimeData(source *models.Source, wg *sync.WaitGroup) {
	f, err := os.Open(source.Settings.SampleFile)
	if err != nil {
		ch.logger.Error(
			"failed to open sample file", zap.String("file", source.Settings.SampleFile), zap.Error(err))
		return
	}

	var r io.Reader
	r = f

	if strings.HasSuffix(source.Settings.SampleFile, ".gz") {
		r, err = gzip.NewReader(f)
		if err != nil {
			ch.logger.Error("failed to read gzip file", zap.String("file", source.Settings.SampleFile), zap.Error(err))
			return
		}
	}

	csvr := csv.NewReader(r)

	var batch []models.CrimeCase

	header_skipped := false
	for {
		record, err := csvr.Read()
		if err == io.EOF {
			ch.logger.Info("finish loading crime case data")
			break
		}

		if !header_skipped {
			header_skipped = true
			continue
		}

		crimeCase := models.CrimeCase{}
		if err := crimeCase.Parse(record); err != nil {
			ch.logger.Error("failed to parse CrimeCase", zap.String("record", strings.Join(record, ",")), zap.Error(err))
			continue
		}

		batch = append(batch, crimeCase)
		if len(batch) >= int(source.Settings.BatchSize) {
			if ch.doCrimeCaseInsert(batch, source.Type) == nil {
				batch = nil
			}
		}
	}

	if len(batch) > 0 {
		ch.doCrimeCaseInsert(batch, source.Type)
	}
}

func (ch *clickHouse) doCrimeCaseInsert(records []models.CrimeCase, typ string) error {
	query := "INSERT INTO default.crimes (lat, lon, description, location_description, primary_type) VALUES (?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for i := range records {
				_, err := stmt.Exec(
					records[i].Lat,
					records[i].Lon,
					records[i].Description,
					records[i].LocationDescription,
					records[i].PrimaryType,
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

func (ch *clickHouse) newCrimeTable(cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP TABLE IF EXISTS default.crimes`); err != nil {
			ch.logger.Error("failed to drop crimes table", zap.Error(err))
			return err
		}
		ch.logger.Info("dropped crimes table")
	}

	_, err := ch.db.Exec(`
		CREATE TABLE IF NOT EXISTS default.crimes (
			lat Float32 CODEC(Gorilla, LZ4HC(9)),
			lon Float32 CODEC(Gorilla, LZ4HC(9)),
			description String,
			location_description String,
			primary_type LowCardinality(String)
		) ENGINE = MergeTree()
		ORDER BY (primary_type)
	`)
	if err != nil {
		ch.logger.Error("failed to create crimes table", zap.Error(err))
	}
	return err
}
