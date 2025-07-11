package main

import (
	"fmt"
	"os"

	"gitlab.com/chenziliang/dataloader/models"
	"gitlab.com/chenziliang/dataloader/sinks"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"

	// // side effects
	_ "gitlab.com/chenziliang/dataloader/sinks/kafka"
	_ "gitlab.com/chenziliang/dataloader/sinks/proton"
	_ "gitlab.com/chenziliang/dataloader/sinks/rockset"

	// side effects
	_ "github.com/timeplus-io/proton-go-driver/v2"
	"gitlab.com/chenziliang/pkg-go/utils"
)

var (
	commit  = ""
	branch  = ""
	buildos = ""
	version = ""
)

func main() {
	app := kingpin.New("dataloader", "load data to target systems")

	configFile := app.Flag("config", "data loader config file").Required().String()

	kingpin.Version("1.0.0")
	kingpin.MustParse(app.Parse(os.Args[1:]))

	c, err := models.NewConfigFromFile(*configFile)
	if err != nil {
		fmt.Printf("failed to read configuration file %+v", err)
		return
	}

	logger, err := utils.NewLogger(c.Log.Level, false)
	if err != nil {
		fmt.Printf("failed to create logger %+v", err)
		return
	}

	_ = logger
	sink, err := sinks.NewSink(c, logger)
	if err != nil {
		logger.Error("failed to NewSink", zap.Error(err))
		return
	}

	sink.LoadData()
}
