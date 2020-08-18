package models

import (
	"fmt"
	"math/rand"
	"time"
)

type Metric struct {
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

type LatLon struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
}

type region struct {
	leftUp    LatLon
	rightUp   LatLon
	leftDown  LatLon
	rightDown LatLon
}

var regionMap map[string]region

func init() {
	regionMap = make(map[string]region)

	regionMap["north-of-china"] = region{
		LatLon{40.0, 112.0}, LatLon{40.0, 118.0},
		LatLon{38.0, 112.0}, LatLon{38.0, 118.0},
	}

	regionMap["east-of-china"] = region{
		LatLon{32.0, 118.0}, LatLon{32.0, 122.0},
		LatLon{28.0, 118.0}, LatLon{28.0, 122.0},
	}

	regionMap["middle-of-china"] = region{
		LatLon{32.0, 110.0}, LatLon{32.0, 117.0},
		LatLon{28.0, 110.0}, LatLon{28.0, 117.0},
	}

	// south-of-china
	regionMap["south-of-china"] = region{
		LatLon{24.0, 106.0}, LatLon{24.0, 116.0},
		LatLon{22.0, 106.0}, LatLon{22.0, 116.0},
	}
}

func generateLocation(r region) LatLon {
	v := rand.Float32()
	lat := r.leftDown.Lat + (r.leftUp.Lat-r.leftDown.Lat)*v
	lon := r.leftUp.Lon + (r.rightUp.Lon-r.leftUp.Lon)*v
	return LatLon{lat, lon}
}

func GenerateDeviceLocations(totalDevices uint32) map[string][]LatLon {
	deviceLocations := make(map[string][]LatLon)

	n := int(totalDevices) / len(regionMap)
	if n == 0 {
		n = 1
	}

	for k, v := range regionMap {
		var locations []LatLon
		for i := 0; i < n; i++ {
			locations = append(locations, generateLocation(v))
		}
		deviceLocations[k] = locations
	}
	return deviceLocations
}

func generateTimeSeriesRecord(ts time.Time, devIndex int, region string, location LatLon) Metric {
	return Metric{
		Devicename:  fmt.Sprintf("dev-%d", devIndex),
		Region:      region,
		Version:     "1.0",
		Lat:         location.Lat,
		Lon:         location.Lon,
		Battery:     rand.Float32() * 100,
		Humidity:    uint16(rand.Uint32()) % uint16(100),
		Temperature: int16(rand.Int31()) % int16(100),
		Timestamp:   ts,
	}
}

func GenerateTimeSeriesRecords(totalDevices uint32, locations map[string][]LatLon) []Metric {
	ts := time.Now()

	records := make([]Metric, 0, totalDevices)
	for k := range regionMap {
		for i := 0; i < int(totalDevices)/len(regionMap); i++ {
			records = append(records, generateTimeSeriesRecord(ts, i, k, locations[k][i]))
		}
	}

	return records
}
