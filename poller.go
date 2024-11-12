package main

import (
	"./ccfg"
	"./cdb"
	"./csender"
	"./worker"
	"flag"
	log "github.com/inconshreveable/log15"
	"strconv"
	"time"
)

func main() {
	configPath := flag.String("c", "./config.toml", "Config file location")
	logPath := flag.String("l", "/var/log/poller.log", "Log file")
	flag.Parse()

	cfg := ccfg.New(configPath)

	// Logging
	Log := log.New()
	Log.SetHandler(log.Must.FileHandler(*logPath, log.TerminalFormat()))

	// Load & parse config
	cdb := cdb.New(cfg.Dbuser, cfg.Dbpassword, cfg.Dbsid, cfg.Dbname, cfg.Dbdomain, Log)

	// Channel for messaging
	chanUdp := make(chan string)

	// make udp sender
	sender := csender.New(cfg.Hosts, Log)
	go sender.Listen(chanUdp)

	// make & start snmp-workers
	var workers = make([]*worker.Worker, cfg.WorkersCount)
	for i := 0; i < cfg.WorkersCount; i++ {
		workers[i] = worker.New(Log)
		go workers[i].Start(cfg.Models, chanUdp)
	}

MainLoop:
	for {
		Log.Info("Checking todo size for all workers...")
		for i := 0; i < len(workers); i++ {
			len := workers[i].GetTodoLen()
			if len > 0 {
				Log.Info("Not all jobs done, in work: " + strconv.Itoa(len) + " sleeping 10s...")
				time.Sleep(10 * time.Second)
				continue MainLoop
			}
		}

		Log.Info("No unfinished jobs.")
		switches := cdb.GetSwitches()

		if len(switches) == 0 {
			Log.Error("Problem getting data from alantin.ural.net")
			continue MainLoop
		}

		j := 0
		for _, sw := range switches {
			// send switch to one of workers
			workers[j].AddToQueue(sw)

			j++

			if j == cfg.WorkersCount {
				j = 0
			}
		}
	}
}
