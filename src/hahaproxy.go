package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/astaxie/beego/logs"
)

var log *logs.BeeLogger
var conf *ConfigType
var g_timeout int
var heartbeat int

// set logfile, loglevel
func SetLog() error {
	logjsonconf := `{
		"filename":"log.log", 
		"maxLines":10000, 
		"maxsize":1e11, 
		"daily":true, 
		"maxDays":15, 
		"rotate":true, 
		"perm":600
	}`

	logconfmap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(logjsonconf), logconfmap); err != nil {
		return err
	}

	log = logs.NewLogger(10000)
	logfilename := conf.Get("global", "log")
	if len(logfilename) != 0 {
		logconfmap["filename"] = logfilename
	}

	jsonconf, err := json.Marshal(logconfmap)
	if err != nil {
		return err
	}

	log.SetLogger("file", string(jsonconf))

	loglevelstr := conf.Get("global", "level")
	if len(loglevelstr) == 0 {
		loglevelstr = "7" // Debug
	}

	loglevel, err := strconv.Atoi(loglevelstr)
	if err != nil {
		return fmt.Errorf("logLevel is invalid:", loglevelstr)
	}

	log.SetLevel(loglevel)

	log.Emergency("hahaproxy start!")

	return nil
}

func main() {
	// get global set
	conf, err := NewConfig("proxy.conf")

	if err != nil {
		fmt.Println(err)
		return
	}

	// set log
	if err := SetLog(); err != nil {
		fmt.Println(err)
		return
	}

	// all server store in map
	sermap := make(map[string]*Worker, 32)

	// main logic
	allsess := conf.AllKey()
	for _, sess := range allsess {
		if sess == "global" {
			continue
		}

		oneServer, ok := sermap[sess]
		if !ok {
			oneServer := NewWorker()
			oneServer.Name = sess
			oneServer.Triger = make(chan string, 1)
			sermap[sess] = oneServer
			go oneServer.Start() // start
		} else {
			oneServer.Triger <- "RELOAD"
		}
	}
}
