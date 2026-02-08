package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
	"io"
	"github.com/boltdb/bolt"
	"time"
	"net"
	"encoding/json"
	"bufio"
)

const (
	version		uint	= 	0
)

var (
	shutdownChan		= make(chan int, 1)
	logChan			= make(chan []string, 512)
)

var config struct {
	LogLevel		int		// 1 for debug, 2 for info, 3 for warning
	Database		string
	RuntimeDirectory	string
}

var runtimeInfo struct {
	serverStarted		bool
	controlListen		net.Listener
}

func shutdownWorker() {
	<- shutdownChan
	// TODO: actual shutdown logic here
	pecho("info", "Shutting down...")

	runtimeInfo.controlListen.Close()
	// Runs at last
	close(logChan)
}

func loggingWorker(loglevel chan int) {
	userLevel := <- loglevel
	pecho("debug", "Started logging daemon")
	for incoming := range logChan {
		msgLevel := 0
		switch incoming[0] {
			case "debug":
				msgLevel = 1
			case "info":
				msgLevel = 2
			case "warn":
				msgLevel = 3
			case "crit":
				fmt.Println("Critical: " + "incoming[1]")
				shutdownChan <- 1
				return
		}
		if userLevel <= msgLevel {
			/* SCARY!!!
			This will panic on malformed events...
			We better guard the channel behind a function
			*/
			fmt.Println(
				"[", incoming[0], "]: ",
				incoming[1],
			)
		}
	}
	fmt.Println("The logging daemon has shutdown")
}

func pecho(level string, msg string) {
	logChan <- []string{
		level,
		msg,
	}
}

func handleControlSig(conn net.Conn) {
	pecho("info", "Handling incoming control event")
	sigSlice := []string{}
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var volitaleSlice []string
		line := scanner.Text()
		err := json.Unmarshal([]byte(line), &volitaleSlice)
		if err != nil {
			pecho("warn", "Could not read control signal: " + err.Error())
		}
		sigSlice = append(
			sigSlice,
			volitaleSlice...
		)
	}
}

func listenSignals() {
	err := os.MkdirAll(config.RuntimeDirectory + "/minepak", 0755)
	if err != nil {
		pecho("crit", "Failed to create runtime directory: " + err.Error())
	}
	runtimeInfo.controlListen, err = net.Listen(
		"unix",
		config.RuntimeDirectory + "/minepak/control",
	)
	if err != nil {
		pecho("crit", "Could not listen on control socket: " + err.Error())
	}
	pecho("debug", "Listening control signals")
	for {
		conn, connErr := runtimeInfo.controlListen.Accept()
		if connErr != nil {
			pecho("info", "Signal listener stopped: " + connErr.Error())
			return
		}
		go handleControlSig(conn)
	}
}

func readConf(loglevel chan int) {

	// Set defaults
	config.LogLevel = 2

	rawConfPath := os.Getenv("_minepakConfig")
	if len(rawConfPath) == 0 {
		panic("Did not find anything in $_minepakConfig")
	}
	fd, err := os.OpenFile(
		rawConfPath,
		os.O_RDONLY,
		0700,
	)
	if err != nil {
		if os.IsNotExist(err) {
			panic("Specified configuration does not exist")
		} else {
			panic("Could not open configuration file: " + err.Error())
		}
	}
	defer fd.Close()
	ioRead, ioErr := io.ReadAll(fd)
	if ioErr != nil {
		panic("Could not read read configuration: " + ioErr.Error())
	}

	decode, decodeErr := toml.Decode(string(ioRead), &config)
	if decodeErr != nil {
		panic("Could not decode configuration: " + decodeErr.Error())
	}
	fmt.Println("Unknown configuration: ", decode.Undecoded())
	loglevel <- config.LogLevel
}

func startServerCore(db *bolt.DB) string {
	if runtimeInfo.serverStarted == true {
		return "collision"
	}
	runtimeInfo.serverStarted = true
	var serverKind string
	var serverPath string
	err := db.Batch(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("Core"))
			if bucket == nil {
				pecho("warn", "Aborting start: no core installed")
				return nil
			}
			serverKind = string(bucket.Get([]byte("kind")))
			serverPath = string(bucket.Get([]byte("path")))
			return nil
		},
	)
	if err != nil {
		pecho("warn", "Could not get server information, aborting start: " + err.Error())
	}

	pecho("debug", "Got server information: " + serverKind + " " + serverPath)

	return "finished"
}

func main() {
	var loglevelChan = make(chan int)
	fmt.Println("minepak version", version)
	go loggingWorker(loglevelChan)
	readConf(loglevelChan)
	db, err := bolt.Open(config.Database, 0700, &bolt.Options{Timeout: 15 * time.Second})
	if err != nil {
		pecho("crit", "Could not open database: " + err.Error())
	}
	defer db.Close()
	pecho("debug", "Opened database")
	pecho("debug", "Attempting start")
	go startServerCore(db)
	go listenSignals()


	// Temp: just trigger exit here
	//panic("test")
	time.Sleep(5 * time.Second)
	shutdownChan <- 1

	shutdownWorker()
}