package main

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

type Worker struct {
	Triger chan string // receive operator

	ChCtrl chan byte // reload channel

	ChHbtc chan byte // heart beat control channel

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

func RdJob(cliconn net.Conn, ch chan []byte, chctrl chan byte, ccl chan byte, name string) {
	defer cliconn.Close()

	buf := make([]byte, 10*1024)
	timer := time.NewTimer(time.Second)
	var cnt = 0
	for {
		cliconn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		// fmt.Println(name, "Rd")
		n, err := cliconn.Read(buf)
		if err != nil {
			operr, ok := err.(*net.OpError)
			if ok {
				if operr.Timeout() {
					// fmt.Println("timeout", cnt)
					cnt++
					if cnt < 300 {
						continue
					}
				}
				fmt.Println("RdJob TimeOut!!! cnt", cnt)
			} else {
				fmt.Println("123", err)
			}

			ccl <- 0
			return
		}

		cnt = 0

		if n == 0 {
			// fmt.Println("Shit Read 0!!!")
			continue
		}

		// nnbuf := buf[:n]
		timer.Reset(30 * time.Second)
		select {
		case ch <- buf[0:n]:
			// case ch <- buf:
		case <-chctrl:
			ccl <- 0 // close associated corontine
			return
		case <-ccl:
			ccl <- 0
			return
		case <-timer.C:
			log.Error("chanen is block too long")
			ccl <- 0
			return
		}
	}
}

func WrJob(cliconn net.Conn, ch chan []byte, chctrl chan byte, ccl chan byte, name string) {
	defer cliconn.Close()
	buf := make([]byte, 10*1024)
	var cnt = 0
	for {
		cnt = 0
		select {
		case <-chctrl:
			ccl <- 0
			return
		case <-ccl:
			ccl <- 0
			return
		case buf = <-ch:
			// log.Info("[%s] WritJob buf [%s]", name, buf)
		WRJOBLAB:
			cliconn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			// fmt.Println(name, "Wr")
			_, err := cliconn.Write(buf)
			if err != nil {
				operr, ok := err.(*net.OpError)
				if ok {
					cnt++
					if operr.Timeout() && cnt < 300 {
						goto WRJOBLAB
					}
					fmt.Println("WrJob TimeOut!!! cnt", cnt)
				} else {
					fmt.Println("321", err)
				}

				ccl <- 0
				return
			}
		}
	}
}

func realjob(cliconn net.Conn, serv string, chctrl chan byte) {
	socktype := CheckType(serv)

	serconn, err := net.DialTimeout(socktype, serv, time.Duration(g_timeout)*time.Millisecond)
	if err != nil {
		log.Error("Connect Server error [%s]", err)
		cliconn.Close()
		return
	}

	// set timeout
	ccl := make(chan byte, 4)    // just 4 coroutine
	c2s := make(chan []byte, 32) // cli -> ser
	s2c := make(chan []byte, 32) // ser -> cli

	go RdJob(cliconn, c2s, chctrl, ccl, "readcli")   // read  client
	go RdJob(serconn, s2c, chctrl, ccl, "readserv")  // read  server
	go WrJob(cliconn, s2c, chctrl, ccl, "writecli")  // write client
	go WrJob(serconn, c2s, chctrl, ccl, "writeserv") // write server
}

func AcceptJob(ls net.Listener, chConn chan net.Conn) {
	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Error("Accept Error [%s]\n", err)
			continue
		}

		log.Info("Accept New One!")
		chConn <- conn
	}
}

func HeartBeat(info string, chTrig chan string, chhbt chan byte) {
	// fmt.Println("start Heartbeat:", info)
HBSTART:
	var reflag = false

	for {
		conn, err := net.DialTimeout("tcp", info, 3*time.Second)
		if err != nil {
			time.Sleep(3 * time.Second)
			reflag = true
			continue
		}

		select {
		case <-chhbt:
			conn.Close()
			return
		case <-time.After(time.Second):
		}

		if reflag {
			reflag = false
			chTrig <- "forcereload" // need reload
			log.Error("HeartBeat: [%s] is actived now! need reload", info)
		}

		for {
			select {
			case <-chhbt:
				conn.Close()
				return
			default:
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				_, err := conn.Write([]byte("hb"))
				if err != nil {
					conn.Close()
					goto HBSTART
				}

				time.Sleep(3 * time.Second)
			}
		}
	}
}

func (wk *Worker) Start() {
	serverstring := conf.Get(wk.Name, "server")
	servers := strings.Fields(serverstring)
	sort.Strings(servers)
	wk.Servers = servers[:len(servers)]
	wk.Bind = conf.Get(wk.Name, "bind")
	wk.ChConn = make(chan net.Conn, 128)
	wk.ChCtrl = make(chan byte)
	wk.ChHbtc = make(chan byte, 20)

	listenfd, err := net.Listen(CheckType(wk.Bind), wk.Bind)
	if err != nil {
		log.Error("listen [%s] error!", wk.Bind)
		return
	}

	// start heart beat corontine
	for i := 0; i < len(wk.Servers); i++ {
		go HeartBeat(wk.Servers[i], wk.Triger, wk.ChHbtc)
	}

	// start accept
	go AcceptJob(listenfd, wk.ChConn)

	wk.Dispatch()
}

func (wk *Worker) Reload(forced int) {
	// start clear coroutine
	serverstring := conf.Get(wk.Name, "server")

	oldserverseq := strings.Join(wk.Servers, " ")
	servers := strings.Fields(serverstring)
	sort.Strings(servers)
	newserverseq := strings.Join(servers, " ")

	if 0 == forced {
		if oldserverseq == newserverseq {
			log.Info("Server [%s] donot need rebalance", wk.Name)
			// do not need rebalance
			return
		}
	} else { // forced reload
		log.Error("Force reload!")
	}

	wk.Servers = servers[:len(servers)]

	go func(ch chan byte) {
		for {
			select {
			case ch <- 0:
				time.Sleep(time.Second)
			case <-time.After(5 * time.Second):
				log.Error("corotine is clean over!")
				return
			}
		}
	}(wk.ChCtrl)

	go func(ch chan byte) {
		for {
			select {
			case ch <- 0:
			case <-time.After(10 * time.Second):
				log.Error("HeartBeat corotine is clean over!")
				return
			}
		}
	}(wk.ChHbtc)

	wk.ChCtrl = make(chan byte) // create new ChCtrl
	wk.ChHbtc = make(chan byte) // create new ChCtrl

	wk.Index = 0

	// start new heart beat corontine(s)
	for i := 0; i < len(wk.Servers); i++ {
		go HeartBeat(wk.Servers[i], wk.Triger, wk.ChHbtc)
	}
}

func (wk *Worker) Dispatch() {
	log.Debug("Dispatch")
	for {
		select {
		case cmd := <-wk.Triger:
			if cmd == "reload" {
				wk.Reload(0)
			} else if cmd == "forcereload" {
				wk.Reload(1)
			}
		case conn := <-wk.ChConn:
			go realjob(conn, wk.Servers[wk.Index], wk.ChCtrl)
			wk.Index++
			wk.Index %= len(wk.Servers)
			// log.Info("[%s] no new client", wk.Name)
		}
	}
}
