package clickhouse

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

type dataPoint struct {
	Devicename  string    `db:"devicename"`
	Region      string    `db:"region"`
	Version     string    `db:"version"`
	Lat         float32   `db:"lat"`
	Lon         float32   `db:"lon"`
	Battery     float32   `db:"battery"`
	Humidity    uint16    `db:"humidity"`
	Temperature int16     `db:"temperature"`
	Timestamp   time.Time `db:"timestamp"`
}

func newTimeSeriesTable(db *sqlx.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS default.devices (
			devicename String,
			region String,
			version String,
			lat Float32 CODEC(LZ4HC(9)),
			lon Float32 CODEC(LZ4HC(9)),
			battery Float32 CODEC(LZ4HC),
			humidity UInt16 CODEC(LZ4HC),
			temperature Int16 CODEC(LZ4HC),
			timestamp DateTime CODEC(LZ4HC)
		) ENGINE = MergeTree()
		ORDER BY (devicename, timestamp)
		PARTITION BY (region, toYYYYMM(timestamp))
	`)
	return err
}

// North-of-China
// (40, 112) -> (40, 118)
// (38, 112) -> (38, 118)

// East-of-China
// (32, 118) -> (32, 122)
// (28, 118) -> (28, 122)

// Middle-of-China
// (32, 110) -> (32, 117)
// (28, 110) -> (28, 117)

// South-of-China
// (24, 106) -> (24, 116)
// (22, 106) -> (22, 116)

type latLon struct {
	lat float32
	lon float32
}

type region struct {
	leftUp    latLon
	rightUp   latLon
	leftDown  latLon
	rightDown latLon
}

var regionMap map[string]region

func init() {
	regionMap = make(map[string]region)

	regionMap["north-of-china"] = region{
		latLon{40.0, 112.0}, latLon{40.0, 118.0},
		latLon{38.0, 112.0}, latLon{38.0, 118.0},
	}

	regionMap["east-of-china"] = region{
		latLon{32.0, 118.0}, latLon{32.0, 122.0},
		latLon{28.0, 118.0}, latLon{28.0, 122.0},
	}

	regionMap["middle-of-china"] = region{
		latLon{32.0, 110.0}, latLon{32.0, 117.0},
		latLon{28.0, 110.0}, latLon{28.0, 117.0},
	}

	// south-of-china
	regionMap["south-of-china"] = region{
		latLon{24.0, 106.0}, latLon{24.0, 116.0},
		latLon{22.0, 106.0}, latLon{22.0, 116.0},
	}
}

func generateLocation(r region) latLon {
	v := rand.Float32()
	lat := r.leftDown.lat + (r.leftUp.lat-r.leftDown.lat)*v
	lon := r.leftUp.lon + (r.rightUp.lon-r.leftUp.lon)*v
	return latLon{lat, lon}
}

func generateDeviceLocations(totalDevices uint32) map[string][]latLon {
	deviceLocations := make(map[string][]latLon)

	n := int(totalDevices) / len(regionMap)
	if n == 0 {
		n = 1
	}

	for k, v := range regionMap {
		var locations []latLon
		for i := 0; i < n; i++ {
			locations = append(locations, generateLocation(v))
		}
		deviceLocations[k] = locations
	}
	return deviceLocations
}

func generateTimeSeriesRecord(ts time.Time, devIndex int, region string, location latLon) dataPoint {
	return dataPoint{
		Devicename:  fmt.Sprintf("dev-%d", devIndex),
		Region:      region,
		Version:     "1.0",
		Lat:         location.lat,
		Lon:         location.lon,
		Battery:     rand.Float32() * 100,
		Humidity:    uint16(rand.Uint32()) % uint16(100),
		Temperature: int16(rand.Int31()) % int16(100),
		Timestamp:   ts,
	}
}

func generateTimeSeriesRecords(totalDevices uint32, locations map[string][]latLon) []dataPoint {
	ts := time.Now()

	records := make([]dataPoint, 0, totalDevices)
	for k := range regionMap {
		for i := 0; i < int(totalDevices)/len(regionMap); i++ {
			records = append(records, generateTimeSeriesRecord(ts, i, k, locations[k][i]))
		}
	}

	return records
}
