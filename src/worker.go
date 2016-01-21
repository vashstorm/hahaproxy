package main

import (
	"net"
	"sort"
	"strings"
	"time"
)

type Worker struct {
	Triger chan string // receive operator

	ChCtrl chan int

	Name    string
	Bind    string
	Servers []string
	Index   int

	// Listenfd net.Listener
	ChConn chan net.Conn
}

func CheckType(bind string) string {
	// return "unix"
	return "tcp"
}

func NewWorker() *Worker {
	worker := new(Worker)
	worker.Index = 0

	return worker
}

func ReadJob(cliconn net.Conn, ch chan string, ccl chan byte) {
	buf := make([]byte, 1024)
	for {
		cliconn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := cliconn.Read(buf)
		if err != nil {
			log.Debug("corotine [%s]", err)
			return
		}

		buf = buf[:n]
		select {
		case ch <- string(buf):
		case <-time.After(3 * time.Second):
			break
		}
	}

	ccl <- 0
}

func WriteJob(cliconn net.Conn, ch chan string, ccl chan byte) {
	for {
		select {
		case buf := <-ch:
			cliconn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_, err := cliconn.Write([]byte(buf))
			if err != nil {
				break
			}
		case <-time.After(3 * time.Second):
			break
		}
	}

	ccl <- 0
}

func realjob(cliconn net.Conn, serv string, chctrl chan int) {
	socktype := CheckType(serv)
	defer cliconn.Close()
	serconn, err := net.DialTimeout(socktype, serv, time.Duration(g_timeout)*time.Millisecond)
	if err != nil {
		log.Error("Connect Server error [%s]", err)
		return
	}

	defer serconn.Close()
	// set timeout
	ccl := make(chan byte, 4)   // just 4 coroutine
	c2s := make(chan string, 8) // cli -> ser
	s2c := make(chan string, 8) // ser -> cli

	go ReadJob(cliconn, c2s, ccl)  // read  client
	go ReadJob(serconn, s2c, ccl)  // read  server
	go WriteJob(cliconn, s2c, ccl) // write client
	go WriteJob(serconn, c2s, ccl) // write server

	for {
		select {
		case <-chctrl: // woker reload
			break
		case <-ccl: // connect has error or timeout
			break
		case <-time.After(3 * time.Second):
			log.Info("NoMessage!")
		}
	}
}

func AcceptJob(ls net.Listener, chConn chan net.Conn) {
	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Error("Accept Error [%s]\n", err)
			continue
		}

		chConn <- conn
	}
}

func (wk *Worker) Init() {
	serverstring := conf.Get(wk.Name, "server")
	if len(serverstring) == 0 {
		log.Error("%s no server! quit!", wk.Name)
		return
	}

	if len(wk.Servers) != 0 {
		oldserverseq := strings.Join(wk.Servers, " ")
		servers := strings.Fields(serverstring)
		sort.Strings(servers)
		newserverseq := strings.Join(servers, " ")

		if oldserverseq == newserverseq {
			// do not need rebalance
		} else {
			wk.Reload()
		}
	} else { // just first start
		wk.Start()
	}
}

func (wk *Worker) Start() {
	serverstring := conf.Get(wk.Name, "server")
	servers := strings.Fields(serverstring)
	sort.Strings(servers)
	wk.Servers = servers[:len(servers)]
	wk.Bind = conf.Get(wk.Name, "bind")
	wk.ChConn = make(chan net.Conn, 128)
	wk.ChCtrl = make(chan int)

	listenfd, err := net.Listen(CheckType(wk.Bind), wk.Bind)
	if err != nil {
		log.Error("listen [%s] error!", wk.Bind)
		return
	}

	// start accept
	go AcceptJob(listenfd, wk.ChConn)

	wk.Dispatch()
}

func (wk *Worker) Reload() {

	// start clear coroutine
	go func(ch chan int) {
		for {
			select {
			case ch <- 0:
				time.Sleep(time.Second)
			case <-time.After(5 * time.Second):
				log.Error("corotine is clean over!\n")
				break
			}
		}
	}(wk.ChCtrl)

	wk.ChCtrl = make(chan int) // create new ChCtrl
	wk.Index = 0
}

func (wk *Worker) Dispatch() {
	for {
		select {
		case cmd := <-wk.Triger:
			if cmd == "reload" {
				wk.Reload()
			}
		case conn := <-wk.ChConn:
			go realjob(conn, wk.Servers[wk.Index], wk.ChCtrl)
			wk.Index++
			wk.Index %= len(wk.Servers)
		case <-time.After(10 * time.Second):
			log.Info("[%s] no new client\n", wk.Name)
		}
	}
}
