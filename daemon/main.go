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
	"math/rand"
	"strconv"
)

const (
	version		uint	= 	0
)

var (
	shutdownChan		= make(chan int, 1)
	logChan			= make(chan []string, 512)
	startCoreChan		= make(chan int)
)

var config struct {
	LogLevel		int		// 1 for debug, 2 for info, 3 for warning
	Database		string
	RuntimeDirectory	string
	TemporaryDirectory	string
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

/*
	For now the proposal for first batch of data is listed below:
		Element 1	Defines the actual action, like install or something
		Element [2:]	whatever flags and control data
*/
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
		break
	}
	if len(sigSlice) > 0 {
		control := sigSlice[0]
		pecho("debug", "Got signal: " + control)
		switch control {
			case "start":
				pecho("info", "Attempting server start...")
				startCoreChan <- 1
				pecho("debug", "Dispatched start job")

			default:
				pecho("warn", "Unknown control signal: " + control)
		}
	} else {
		pecho("warn", "Could not handle signal: empty data")
		return
	}
}

func pickTempDir() string {
	if config.TemporaryDirectory == "" {
		pecho(
		"warn",
		"Could not pick temporary directory because config.TemporaryDirectory is invalid",
		)
		stat, err := os.Stat("/tmp")
		if err != nil || stat.IsDir() == false {
			pecho("crit", "Could not find a suitable temporary directory")
			return ""
		}

		for {
			randomDir := strconv.Itoa(rand.Intn(2147483647))
			err = os.Mkdir("/tmp/" + randomDir, 0700)
			if err != nil {
				continue
			} else {
				return "/tmp/" + randomDir
			}
		}
	} else {
		stat, err := os.Stat(config.TemporaryDirectory)
		if err != nil || stat.IsDir() == false {
			pecho("crit", "TemporaryDirectory unusable")
			return ""
		}
		for {
			randomDir := strconv.Itoa(rand.Intn(2147483647))
			path := config.TemporaryDirectory + randomDir
			err = os.Mkdir(path, 0700)
			if err != nil {
				continue
			} else {
				pecho("debug", "Picked temp directory: " + path)
				return path
			}
		}
	}
}

func pickStreamSock() (conn net.Conn, sock string) {
	var trials uint
	for {
		if trials > 2147483647 {
			pecho("warn", "Could not pick a stream socket: no available name")
			return
		}
		sockPath := config.RuntimeDirectory + "/minepak/stream-" + strconv.Itoa(
			rand.Intn(2147483647),
		)
		listener, err := net.Listen("unix", sockPath)
		if err != nil {
			trials++
			pecho("debug", "Could not listen for data: " + err.Error())
		} else {
			conn, err = listener.Accept()
			if err != nil {
				pecho("warn", "Could not receive data: " + err.Error())
				sock = "Could not receive data: " + err.Error()
				conn = nil
				return
			}
		}
	}

}

func failBack(conn net.Conn)

// Notify the other end to send data, then receive

/*
	Sender should do this:
	var size int64
	binary.Read(conn, binary.BigEndian, &size)

	This is cursed, should do JSON over HTTP
*/

func installPackageFromSocket(conn net.Conn) {
	ready, _ := json.Marshal("send-package-data")
	conn.Write([]byte(ready))
	pecho("debug", "Receiving data from client...")
	tempPath := pickTempDir()
	fd, err := os.OpenFile(
		tempPath + "pack.file",
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0700,
	)
	if err != nil {
		pecho("warn", "Could not receive package: " + err.Error())
		var errMsg = []string{
			"fail",
			"Daemon could not receive package: " + err.Error(),
		}
		jsonObj, _ := json.Marshal(errMsg)
		conn.Write(jsonObj)
	}
	connStream, sock := pickStreamSock()
	if connStream == nil {
		var errMsg = []string{
			"fail",
			sock,
		}
		jsonObj, _ := json.Marshal(errMsg)
		conn.Write(jsonObj)
		return
	}
	var errMsg = []string{
		"fail",
		sock,
	}
	jsonObj, _ := json.Marshal(errMsg)
	conn.Write(jsonObj)
	pecho("debug", "Sent streaming socket")
	dataCount, errIO := io.Copy(fd, connStream) // Note: other end should close conn
	if errIO != nil {
		errMsgp := "Could not receive data: " + errIO.Error()
		pecho("warn", errMsgp)
		var errMsg = []string{
			"fail",
			errMsgp,
		}
		jsonObj, _ := json.Marshal(errMsg)
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
	for {
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



	<- startCoreChan
	}
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