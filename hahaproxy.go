package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"syscall"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/howeyc/fsnotify"
)

var log *logs.BeeLogger
var conf *ConfigType
var g_timeout int = 4
var heartbeat int
var conffile = "proxy.conf"

// set logfile, loglevel
func SetLog() error {
	logjsonconf := `{
		"filename":"log.log",
		"maxLines":900000,
		"daily":false,
		"maxDays":15,
		"rotate":true,
		"perm":600
	}`

	// "maxsize":1e10,
	var logconfmap map[string]interface{}
	if err := json.Unmarshal([]byte(logjsonconf), &logconfmap); err != nil {
		return err
	}

	log = logs.NewLogger(10000)

	logfilename := conf.Get("global", "log")
	if len(logfilename) != 0 {
		logconfmap["filename"] = logfilename
	}

	fmt.Printf("log file is [%s]\n", logfilename)

	jsonconf, err := json.Marshal(logconfmap)
	if err != nil {
		fmt.Println("HHH", err)
		return err
	}

	log.SetLogger("file", string(jsonconf))
	log.EnableFuncCallDepth(true)

	loglevelstr := conf.Get("global", "level")
	if len(loglevelstr) == 0 {
		loglevelstr = "8" // Debug
	}

	loglevel, err := strconv.Atoi(loglevelstr)
	if err != nil {
		return fmt.Errorf("logLevel is invalid:", loglevelstr)
	}

	log.SetLevel(loglevel)

	log.Emergency("hahaproxy start!")

	return nil
}

func monitorConf(file string, wa *fsnotify.Watcher) {
	for {
		err := wa.Watch(file)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		log.Error("Set Watch [%s] ok!", file)
		break
	}
}

func main() {
	chhh := make(chan os.Signal)
	signal.Notify(chhh, syscall.SIGINT)

	f, _ := os.Create("profile.log")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// get global set
	var err error
	conf, err = NewConfig(conffile)

	if err != nil {
		fmt.Println(err)
		return
	}

	// teststr := conf.Get("global", "log")
	// fmt.Printf("teststr [%s], len(teststr) [%d]\n", teststr, len(teststr))

	fmt.Println("Load Config Over!")

	// set log
	if err := SetLog(); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Log is Okay!")

	// all server store in map
	sermap := make(map[string]*Worker, 32)

	// register inotify
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error("fsnotify.NewWatcher Error! [%s]", err)
		return
	}
	go monitorConf(conffile, watcher)

MAINLOGIC:
	// main logic
	allsess := conf.AllKey()
	for _, sess := range allsess {
		if sess == "global" {
			continue
		}

		oneServer, ok := sermap[sess]
		if !ok {
			log.Info("start new Server [%s]", sess)
			oneServer := NewWorker()
			oneServer.Name = sess
			oneServer.Triger = make(chan string, 1)
			sermap[sess] = oneServer
			go oneServer.Start() // start
		} else {
			log.Info("reload Server [%s]", sess)
			oneServer.Triger <- "reload"
		}
	}

	// main loop

	for {
		select {
		case ev := <-watcher.Event:
			if ev.IsModify() || ev.IsCreate() {
				fmt.Println("Modify Event")
				conf, err = NewConfig(conffile)
				if err != nil {
					log.Error("Reload Error! [%s]", err)
					continue
				}
				goto MAINLOGIC
			} else if ev.IsDelete() {
				fmt.Println("Delete Event")
				go monitorConf(conffile, watcher)
			}

		case <-time.After(10 * time.Second):
			// log.Info("Main coroutine loop")

		case <-chhh:
			return
		}
	}
}
