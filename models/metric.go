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

var regionMap map[string][]region

func init() {
	regionMap = make(map[string][]region)

	regionMap["north-of-china"] = []region{
		region{
			// Beijing
			LatLon{40.014126, 116.241441}, LatLon{40.014126, 116.527459},
			LatLon{39.835775, 116.241441}, LatLon{39.835775, 116.527459},
		},
		region{
			// Tangshan
			LatLon{39.691410, 118.111948}, LatLon{39.691410, 118.391853},
			LatLon{39.524439, 118.111948}, LatLon{39.524439, 118.391853},
		},
		region{
			// Shijiazhuang
			LatLon{38.086184, 114.429500}, LatLon{38.086184, 114.605516},
			LatLon{37.914092, 114.429500}, LatLon{37.914092, 114.605516},
		},
		region{
			// Zhangjiakou
			LatLon{40.785688, 114.782518}, LatLon{40.785688, 115.064669},
			LatLon{40.612707, 114.782518}, LatLon{40.612707, 115.064669},
		},
		region{
			// Baoding
			LatLon{38.928030, 115.339878}, LatLon{38.928030, 115.617934},
			LatLon{38.754821, 115.339878}, LatLon{38.754821, 115.617934},
		},
	}

	regionMap["east-of-china"] = []region{
		region{
			// Shanghai
			LatLon{31.274066, 121.338275}, LatLon{31.274066, 121.614325},
			LatLon{31.103254, 121.338275}, LatLon{31.103254, 121.614325},
		},
		region{
			// Suzhou
			LatLon{31.315470, 120.517825}, LatLon{31.315470, 120.794325},
			LatLon{31.143254, 120.517825}, LatLon{31.143254, 120.794325},
		},
		region{
			// Hangzhou
			LatLon{30.318831, 120.104833}, LatLon{30.318831, 120.384325},
			LatLon{30.143254, 120.104833}, LatLon{30.143254, 120.384325},
		},
		region{
			// Ningbo
			LatLon{29.896547, 121.499437}, LatLon{29.896547, 121.774325},
			LatLon{29.723254, 121.499437}, LatLon{30.143254, 121.774325},
		},
		region{
			// Nanjing
			LatLon{32.061658, 118.740244}, LatLon{32.061658, 119.020244},
			LatLon{31.891658, 118.740244}, LatLon{31.891658, 119.024325},
		},
	}

	regionMap["middle-of-china"] = []region{
		region{
			// Chengdu
			LatLon{30.700643, 103.965655}, LatLon{30.700643, 104.245655},
			LatLon{30.630643, 103.965655}, LatLon{30.630643, 104.245655},
		},
		region{
			// Chongqing
			LatLon{29.489695, 106.843546}, LatLon{29.489695, 107.123546},
			LatLon{29.319695, 106.843546}, LatLon{29.319695, 107.123546},
		},
		region{
			// Changsha
			LatLon{28.231280, 112.897382}, LatLon{28.231280, 113.177382},
			LatLon{28.061280, 112.897382}, LatLon{28.061280, 113.177382},
		},
		region{
			// Wuhan
			LatLon{30.639306, 114.074692}, LatLon{30.639306, 114.074692},
			LatLon{30.469306, 114.074692}, LatLon{30.469306, 114.074692},
		},
		region{
			// Nanchang
			LatLon{28.704473, 115.775266}, LatLon{28.704473, 116.055266},
			LatLon{28.534473, 115.775266}, LatLon{28.534473, 116.055266},
		},
	}

	// south-of-china
	regionMap["south-of-china"] = []region{
		region{
			// Guangzhou
			LatLon{23.140674, 113.232065}, LatLon{23.140674, 113.512065},
			LatLon{22.970674, 113.232065}, LatLon{22.970674, 113.512065},
		},
		region{
			// Dongguan
			LatLon{23.072614, 113.634543}, LatLon{23.072614, 113.914543},
			LatLon{22.900674, 113.634543}, LatLon{22.900674, 113.914543},
		},
		region{
			// Nanning
			LatLon{22.855866, 108.249357}, LatLon{22.855866, 108.529357},
			LatLon{22.685866, 108.249357}, LatLon{22.685866, 108.529357},
		},

		region{
			// Shaoguan
			LatLon{24.832381, 113.514673}, LatLon{24.832381, 113.794673},
			LatLon{24.662381, 113.514673}, LatLon{24.662381, 113.794673},
		},
	}
}

func generateLocation(regions []region) LatLon {
	rand.Seed(time.Now().UTC().UnixNano())
	r := regions[rand.Uint32()%uint32(len(regions))]

	latDelta := r.leftUp.Lat - r.leftDown.Lat
	lonDelta := r.rightUp.Lon - r.leftUp.Lon

	lat := r.leftDown.Lat + latDelta*rand.Float32()
	lon := r.leftUp.Lon + lonDelta*rand.Float32()

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
