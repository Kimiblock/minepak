package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
	"io"
	"github.com/boltdb/bolt"
	"time"
	"net"
	"net/http"
	"mime/multipart"
	"encoding/json"
	"bufio"
	"math/rand"
	"strconv"
	"compress/gzip"
	"archive/tar"
	"path/filepath"
	"os/exec"
	"strings"
)

const (
	version		uint	= 	0
	mpBound		string	=	"top.kimiblock.minepak.boundary-sus"
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
	JavaPath		string
	JvmArgs			string
}

var runtimeInfo struct {
	serverStarted		bool
	controlListen		net.Listener
}

type response struct {
	success		bool;
	log		string;
}

type pkgInfo struct {
	name		string;
	core		bool;
	installed	bool;
	flavor		string;
	requireCore	string;
	version		string;
	epoch		int;
	depends		[]string;
	configs		[]string;
}

type dbInfo struct {
	db		*bolt.DB
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

// Peer MUST check installed is true!
func checkDbforPkg(dbconn *bolt.DB, pkgname string) (returnInfo pkgInfo) {
	returnInfo.name = pkgname
	dbconn.View(func(tx *bolt.Tx) error {
		sysBuck := tx.Bucket([]byte("Packages"))
		if sysBuck == nil {
			pecho("warn", "Could not query system database: ")
			return nil
		} else {
			rawVal := sysBuck.Get([]byte(pkgname))
			if rawVal == nil || string(rawVal) != "true" {
				pecho("debug", "Package state: uninstalled from database")
				returnInfo.installed = false
				return nil
			}
		}
		returnInfo.installed = true
		bucket := tx.Bucket([]byte(pkgname))
		if bucket != nil {
			core, err := strconv.ParseBool(string(bucket.Get([]byte("core"))))
			if err != nil {
				pecho("warn", "Treating unknown core status as false")
			}
			if core == true {
				returnInfo.core = true
			}
			returnInfo.flavor = string(bucket.Get([]byte("flavor")))
			returnInfo.requireCore = string(bucket.Get([]byte("requireCore")))
			returnInfo.version = string(bucket.Get([]byte("version")))
			returnInfo.epoch, err = strconv.Atoi(string(bucket.Get([]byte("epoch"))))
			if err != nil {
				pecho("warn", "Unable to read epoch")
				returnInfo.epoch = 0
			}
			err = json.Unmarshal(bucket.Get([]byte("depends")), &returnInfo.depends)
			if err != nil {
				pecho("warn", "Unable to unmarshal depends: " + err.Error())
			}
			err = json.Unmarshal(bucket.Get([]byte("configs")), &returnInfo.configs)
			if err != nil {
				pecho("warn", "Unable to unmarshal configs: " + err.Error())
			}
		} else {
			returnInfo.installed = false
		}
		return nil
	})
	return
}

/*
	Used to retrieve info from a package database
	Peer must check valid!
*/
func checkPkgData(dbpath string) (returnInfo pkgInfo, valid bool, fileMap map[string]string) {
	dbconn, err := bolt.Open(dbpath, 0700, nil)
	if err != nil {
		pecho("warn", "Could not open package database: " + err.Error())
		return
	}
	dbconn.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("metadata"))
		if bucket != nil {
			returnInfo.name = string(bucket.Get([]byte("name")))
			if len(returnInfo.name) == 0 {
				pecho("warn", "Invalid package name: empty string")
				return nil
			} else if strings.Contains(returnInfo.name, " ") {
				pecho("warn", "Package name must not contain spaces")
				return nil
			}
			var core bool
			core, err = strconv.ParseBool(string(bucket.Get([]byte("core"))))
			if err != nil {
				pecho("warn", "Treating unknown core status as false")
			}
			if core == true {
				returnInfo.core = true
			}
			returnInfo.flavor = string(bucket.Get([]byte("flavor")))
			returnInfo.requireCore = string(bucket.Get([]byte("requireCore")))
			returnInfo.version = string(bucket.Get([]byte("version")))
			returnInfo.epoch, err = strconv.Atoi(string(bucket.Get([]byte("epoch"))))
			if err != nil {
				pecho("warn", "Unable to read epoch")
				returnInfo.epoch = 0
			}
			err = json.Unmarshal(bucket.Get([]byte("depends")), &returnInfo.depends)
			if err != nil {
				pecho("warn", "Unable to unmarshal depends: " + err.Error())
			}
			err = json.Unmarshal(bucket.Get([]byte("configs")), &returnInfo.configs)
			if err != nil {
				pecho("warn", "Unable to unmarshal configs: " + err.Error())
			}
		} else {
			pecho("warn", "Malformed database: no metadata section")
			return nil
		}

		bucketName := "files"
		bucket = tx.Bucket([]byte(bucketName))
		if bucket == nil {
			pecho("warn", "Could not read package files: bucket missing")
			return nil
		}
		cursor := bucket.Cursor()
		for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
			fileMap[string(key)] = string(val)
		}
		valid = true
		return nil
	})
	return
}

// Writes package metadata to database, and check for conflicts
func instPkgDb(info pkgInfo, db *bolt.DB, filesMap map[string]string) (success bool) {
	var coreInstalled bool
	db.View(func(tx *bolt.Tx) error {
		sysBuck := tx.Bucket([]byte("System"))
		if sysBuck == nil {
			coreInstalled = false
			pecho("debug", "Did not find any core in database")
		} else {
			if len(sysBuck.Get([]byte("core"))) == 0 {
				coreInstalled = false
				pecho("debug", "Did not find any core in database")
			} else {
				coreInstalled = true
			}
		}
		bucket := tx.Bucket([]byte(info.name))
		if bucket != nil {
			pecho("warn", "Could not install: another package present")
			success = false
			return nil
		}

		cur := tx.Cursor()
		for key, val := cur.First(); key != nil; key, val = cur.Next() {
			for _, v := range filesMap {
				if v == string(val) {
					pecho("warn", "Collision on " + v + " of object " + string(key))
					success = false
					return nil
				}
			}
		}
		success = true
		return nil
	})

	pecho("info", "Updating database metadata")
	db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte(info.name))
		if err != nil {
			pecho("warn", "Could not install: failed to create a new bucket: " + err.Error())
		}
		metaBuck, err := bucket.CreateBucket([]byte("files"))
		if err != nil {
			pecho("warn", "Could not install: failed to create a new bucket: " + err.Error())
			success = false
			return nil
		}

		configsJson, err := json.Marshal(info.configs)
		if err != nil {
			pecho("warn", "Could not marshal depends info: " + err.Error())
			success = false
			return nil
		}
		dependsJson, err := json.Marshal(info.depends)
		if err != nil {
			pecho("warn", "Could not marshal depends info: " + err.Error())
			success = false
			return nil
		}

		if len(info.name) > 100 {
			pecho("warn", "Invalid package: name too long")
			success = false
			return nil
		}

		// Map for database internal structure
		var infoMap = map[string]string{
			"name":			info.name,
			"version":		info.version,
			"epoch":		strconv.Itoa(info.epoch),
			"installed":		strconv.FormatBool(info.installed),
			"flavor":		info.flavor,
			"core":			strconv.FormatBool(info.core),
			"depends":		string(dependsJson),
			"configs":		string(configsJson),
		}

		if info.core == false {
			infoMap["requireCore"] = info.requireCore
		} else {
			bk, err := tx.CreateBucketIfNotExists([]byte("System"))
			if err != nil {
				pecho("warn", "Could not create system bucket: " + err.Error())
				success = false
				return nil
			} else {
				if coreInstalled == false {
					bk.Put([]byte("core"), []byte(info.name))
				} else {
					coreExist := string(bk.Get([]byte("core")))
					pecho("warn", "Core conflict: already installed " + coreExist)
					success = false
					return nil
				}
			}
		}

		for k, v := range infoMap {
			key := []byte(k)
			val := []byte(v)
			err := metaBuck.Put(key, val)
			if err != nil {
				pecho("warn", "Could not update metadata: " + err.Error())
				success = false
				return nil
			}
		}


		filesBuck, err := bucket.CreateBucket([]byte("files"))
		if err != nil {
			pecho("warn", "Could not install: failed to create a new bucket: " + err.Error())
			success = false
			return nil
		}

		var entryCount uint
		for key, val := range filesMap {
			if info.core == true && entryCount > 1 {
				pecho("warn", "Core should only have one entry")
			}
			filesBuck.Put([]byte(key), []byte(val))
			entryCount++
		}

		pkgBuck, err := tx.CreateBucketIfNotExists([]byte("Packages"))
		if err != nil {
			pecho("warn", "Could not create packages bucket: " + err.Error())
			success = false
			return nil
		} else {
			pkgBuck.Put([]byte(info.name), []byte("true"))
		}
		success = true
		return nil
	})
	return
}

func rmPkg (packageName string, dbcore *dbInfo) (resp response) {
	pecho("debug", "Removing package " + packageName)





	resp.success = true
	return
}

// Notify the other end to send data, then receive

/*
	For now there's only one part in a mp message,
	the client should send header minepakType = package
*/
func (dbcore *dbInfo) installPackageFromSocket(writer http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	var resp response
	pecho("debug", "Receiving data from client...")
	tempPath := pickTempDir()
	fd, err := os.OpenFile(
		tempPath + "pack.file",
		os.O_CREATE|os.O_RDWR|os.O_TRUNC,
		0700,
	)
	if err != nil {
		pecho("warn", "Could not receive package: " + err.Error())
		resp.success = false
		resp.log = "Daemon could not receive package: " + err.Error()
		jsonObj, _ := json.Marshal(resp)
		writer.Write(jsonObj)
		return
	}
	defer fd.Close()

	var bytes int64
	bytes, err = io.Copy(fd, req.Body)
	pecho("debug", "Got " + strconv.Itoa(int(bytes)) + " bytes from client")
	fd.Seek(0, io.SeekStart)
	bufReader := bufio.NewReader(fd)
	reader := multipart.NewReader(bufReader, mpBound)

	part, partErr := reader.NextPart()
	if partErr != nil {
		pecho("warn", "Could not read streamed data: " + partErr.Error())
		resp.success = false
		resp.log = "Daemon could not read streamed data: " + partErr.Error()
		jsonObj, _ := json.Marshal(resp)
		writer.Write(jsonObj)
		return
	} else if part.Header.Get("minepakType") != "package" {
		pecho("warn", "Invalid data type received")
		resp.success = false
		resp.log = "Daemon could not read streamed data: " + "Invalid data type received"
		jsonObj, _ := json.Marshal(resp)
		writer.Write(jsonObj)
		return
	}

	gzipReader, gErr := gzip.NewReader(part)
	if gErr != nil {
		pecho("warn", "Could not decompress GZip archive: " + gErr.Error())
		resp.success = false
		resp.log = "Daemon could not decompress GZip archive: " + gErr.Error()
		jsonObj, _ := json.Marshal(resp)
		writer.Write(jsonObj)
		return
	}

	tarReader := tar.NewReader(gzipReader)

	for {
		header, headErr := tarReader.Next()
		if headErr != nil {
			if headErr == io.EOF {
				break
			}
			pecho("warn", "Malformed archive")
			resp.log = "Malformed package"
			resp.success = false
		}
		targetPath := filepath.Join(tempPath, header.Name)
		switch header.Typeflag {
			case tar.TypeDir:
				os.MkdirAll(targetPath, 0700)
			case tar.TypeReg:
				os.MkdirAll(filepath.Dir(targetPath), 0700)
				tgFd, err := os.OpenFile(
					targetPath,
					os.O_CREATE|os.O_TRUNC|os.O_CREATE,
					0700,
				)
				if err != nil {
					pecho("warn", "Could not open file for writing: " + err.Error())
					continue
				}
				io.Copy(tgFd, tarReader)
				tgFd.Close()
			default:
				pecho("warn", "Could not handle header typeflag")
		}
	}

	dbPath := filepath.Join(
		tempPath,
		"top.kimiblock.minepak.package",
		"info",
		"metadata.bolt",
	)

	var info pkgInfo
	var valid bool
	var fileMap = make(map[string]string)

	info, valid , fileMap = checkPkgData(dbPath)
	if valid == false {
		pecho("warn", "checkPkgData returned invalid status")
		resp.log = "Package database is invalid or corrupted"
		resp.success = false
		jsonObj, _ := json.Marshal(resp)
		writer.Write(jsonObj)
		return
	}

	pecho("debug", "Starting installation...")



	objpath := filepath.Join(
		tempPath,
		"object",
	)
	for key, val := range fileMap {
		pecho("debug", "Processing object: " + key)
		srcFd, err := os.OpenFile(
			filepath.Join(objpath, key),
			os.O_RDONLY,
			0700,
		)
		if err != nil {
			if os.IsNotExist(err) {
				pecho(
				"warn",
				"Could not install package: missing object " + key + " for path " + val)
				resp.success = false
				resp.log = "Daemon could not read corrupted package"
				jsonObj, _ := json.Marshal(resp)
				writer.Write(jsonObj)
				return
			}
			resp.success = false
			resp.log = "Daemon could not open object: " + err.Error()
			jsonObj, _ := json.Marshal(resp)
			writer.Write(jsonObj)
			pecho("warn", resp.log)
			return
		}
		defer srcFd.Close()

		err = os.MkdirAll(filepath.Dir(val), 0700)
		if err != nil {
			pecho("warn", "Failed to create directory: " + err.Error())
			resp.success = false
			resp.log = "Daemon failed to create directory: " + err.Error()
			jsonObj, _ := json.Marshal(resp)
			writer.Write(jsonObj)
			return
		}
		dstFd, dstErr := os.OpenFile(val, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0700)
		if dstErr != nil {
			pecho("warn", "Could not open destination: " + dstErr.Error())
			resp.success = false
			resp.log = "Daemon could not open destination: " + dstErr.Error()
			jsonObj, _ := json.Marshal(resp)
			writer.Write(jsonObj)
		}
		defer dstFd.Close()
		var bytes int64
		bytes, err = io.Copy(dstFd, srcFd)
		if err != nil {
			pecho("warn", "I/O error writing file: " + err.Error())
			resp.success = false
			resp.log = "Daemon caught" + "I/O error writing file: " + err.Error()
			jsonObj, _ := json.Marshal(resp)
			writer.Write(jsonObj)
			return
		}
		pecho("debug", "Wrote " + strconv.Itoa(int(bytes)) + " bytes")
	}

	go cleanOnTimer(tempPath, 1*time.Minute)

}

func cleanOnTimer(path string, timer time.Duration) {
	time.Sleep(timer)
	pecho("debug", "Cleaning up " + path)
	err := os.RemoveAll(path)
	if err != nil {
		pecho("warn", "Could not clean path: " + path + ": " + err.Error())
		return
	}
	pecho("debug", "Done cleaning" + path)
}

func unknownSigHandler(writer http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	var resp response
	resp.success = false
	resp.log = "Unknown operation"
	jsonObj, _ := json.Marshal(resp)
	writer.Write(jsonObj)
	url := req.RequestURI
	pecho("warn", "Got unknown signal on " + url)
}

func listenSignals(db *bolt.DB) {
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

	srvFunc := &dbInfo{db: db}

	http.HandleFunc("/", unknownSigHandler)
	http.HandleFunc("/instpkg", srvFunc.installPackageFromSocket)


	http.Serve(runtimeInfo.controlListen, nil)
}

func readConf(loglevel chan int) {
	// Set defaults
	config.LogLevel = 2
	config.JavaPath = "java"

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
	var serverVer string
	err := db.Batch(
		func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("Core"))
			if bucket == nil {
				pecho("warn", "Aborting start: no core installed")
				return nil
			}
			serverKind = string(bucket.Get([]byte("flavor")))
			serverPath = string(bucket.Get([]byte("server-core")))
			serverVer = string(bucket.Get([]byte("epoch"))) + ":" + string(bucket.Get([]byte("version")))
			return nil
		},
	)
	if err != nil {
		pecho("warn", "Could not get server information, aborting start: " + err.Error())
	}

	pecho(
	"debug",
	"Got server information: " + serverKind + " " + serverVer + " @" + serverPath)


	args := strings.Split(config.JvmArgs, " ")


	execCmd := exec.Command(config.JavaPath, args...)
	execCmd.Stdout = os.Stdout
	// This takes away the stdin, but shoul be fine
	execCmd.Stdin = os.Stdin
	execCmd.Stderr = os.Stderr

	pecho("info", "Starting server...")
	execCmd.Run()



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
	go listenSignals(db)


	// Temp: just trigger exit here
	//panic("test")
	time.Sleep(5 * time.Second)
	shutdownChan <- 1

	shutdownWorker()
}