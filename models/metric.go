package models

import (
	"fmt"
	"math/rand"
	"time"
)

type Metric struct {
	Devicename          string    `db:"devicename"`
	Region              string    `db:"region"`
	City                string    `db:"city"`
	Version             string    `db:"version"`
	Lat                 float32   `db:"lat"`
	Lon                 float32   `db:"lon"`
	Battery             float32   `db:"battery"`
	Humidity            uint16    `db:"humidity"`
	Temperature         int16     `db:"temperature"`
	HydraulicPressure   float32   `db:"hydraulic_pressure"`
	AtmosphericPressure float32   `db:"atmospheric_pressure"`
	Timestamp           time.Time `db:"timestamp"`
}

func generateMetric(ts time.Time, devIndex int, region string, location LatLon) Metric {
	rand.Seed(time.Now().UTC().UnixNano())
	return Metric{
		Devicename:          fmt.Sprintf("%s-BHSH-%05d", location.City, devIndex),
		Region:              region,
		City:                location.City,
		Version:             "1.0",
		Lat:                 location.Lat,
		Lon:                 location.Lon,
		Battery:             rand.Float32() * 100,
		Humidity:            uint16(rand.Uint32()) % uint16(100),
		Temperature:         int16(rand.Int31()) % int16(100),
		HydraulicPressure:   1000 + rand.Float32()*1000,
		AtmosphericPressure: 101.3 + rand.Float32()*100,
		Timestamp:           ts,
	}
}

func GenerateMetrics(totalDevices uint32, locations map[string][]LatLon) []Metric {
	ts := time.Now()

	records := make([]Metric, 0, totalDevices)
	for k := range regionMap {
		for i := 0; i < int(totalDevices)/len(regionMap); i++ {
			records = append(records, generateMetric(ts, i, k, locations[k][i]))
		}
	}

	return records
}
