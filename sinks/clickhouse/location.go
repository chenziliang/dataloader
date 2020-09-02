package clickhouse

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/pkg-go/utils"
	"go.uber.org/zap"
)

func (ch *clickHouse) generateLocations(source *models.Source, centerized bool) map[string][]models.LatLon {
	dbFile := source.Settings.LastRunStateDB
	if utils.FileExists(dbFile) {
		if locations, err := ch.loadLocations(dbFile); err == nil {
			return locations
		}
	}

	locations := models.GenerateLocations(source.Settings.TotalEntities, centerized)

	ch.dumpLocations(locations, dbFile)

	return locations
}

func (ch *clickHouse) dumpLocations(locations map[string][]models.LatLon, dbFile string) error {
	// save the location information for next use
	data, err := json.Marshal(&locations)
	if err != nil {
		ch.logger.Info("failed to marshal location info", zap.Error(err))
		return err
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		ch.logger.Error("failed to compress location data", zap.Error(err))
		return err
	}
	zw.Close()

	err = ioutil.WriteFile(dbFile, buf.Bytes(), 0644)
	if err != nil {
		ch.logger.Error("failed to persitentdevice location information in db", zap.Error(err))
	} else {
		ch.logger.Info("persitented location information in db")
	}
	return err
}

func (ch *clickHouse) loadLocations(dbFile string) (map[string][]models.LatLon, error) {
	ch.logger.Info("loading locations", zap.String("location_db", dbFile))

	data, err := ioutil.ReadFile(dbFile)
	if err != nil {
		ch.logger.Error("failed to read location db file", zap.Error(err))
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(data)

	zr, err := gzip.NewReader(&buf)
	if err != nil {
		ch.logger.Error("failed to create gzip reader", zap.Error(err))
		return nil, err
	}
	defer zr.Close()

	data, err = ioutil.ReadAll(zr)
	if err != nil {
		ch.logger.Error("failed to decompress data", zap.Error(err))
		return nil, err
	}

	var locations map[string][]models.LatLon
	err = json.Unmarshal(data, &locations)
	if err != nil {
		ch.logger.Error("failed to load device location db", zap.Error(err))
	} else {
		ch.logger.Info("successfully load location db")
	}
	return locations, err
}
