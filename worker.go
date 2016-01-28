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

	return worker
}

func OneStream(conn1 net.Conn, conn2 net.Conn, chctrl chan byte, ccl chan byte) {
	defer conn1.Close()
	defer conn2.Close()

	buf := make([]byte, 10*1024)
	// timer := time.NewTimer(time.Second)
	var cnt = 0
	for {
		select {
		case <-chctrl:
			ccl <- 0 // close associated corontine
			return
		case <-ccl:
			ccl <- 0
			return
		default:
			// read from conn1
			conn1.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
			// fmt.Println(name, "Rd")
			n, err := conn1.Read(buf)
			if err != nil {
				if operr, ok := err.(*net.OpError); ok {
					if operr.Timeout() {
						cnt++
						if cnt < 30 {
							continue
						}
					}
				}

				ccl <- 0
				return
			}

			cnt = 0

			if n == 0 {
				// fmt.Println("Shit Read 0!!!")
				continue
			}

			// write to conn2
			newbf := buf[:n]
		STARTWR:
			conn2.SetWriteDeadline(time.Now().Add(1000 * time.Millisecond))
			_, err = conn2.Write(newbf)
			if err != nil {
				operr, ok := err.(*net.OpError)
				if ok {
					cnt++
					if operr.Timeout() && cnt < 30 {
						goto STARTWR
					}
				}

				ccl <- 0
				return
			}
		}
	}
}

func (wk *Worker) RealJob(cliconn net.Conn, serv string) {
	socktype := CheckType(serv)

	serconn, err := net.DialTimeout(socktype, serv, time.Duration(2*time.Second))
	if err != nil {
		// make sure not so fast
		if operr, ok := err.(*net.OpError); ok {
			if !operr.Timeout() && !operr.Temporary() {
				time.Sleep(500 * time.Millisecond)
			}
		}

		select {
		case wk.ChConn <- cliconn:
			// fmt.Println("reentry accept channel failed host", serv)
		case <-time.After(time.Second):
		}
		return
	}

	// set timeout
	ccl := make(chan byte, 2) // just 4 coroutine

	go OneStream(cliconn, serconn, wk.ChCtrl, ccl) // client -> server
	go OneStream(serconn, cliconn, wk.ChCtrl, ccl) // server -> client
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
	var reflag = false
HBSTART:

	for {
		conn, err := net.DialTimeout("tcp", info, 3*time.Second)
		if err != nil {
			// log.Error("%s", err)
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

		respbuf := make([]byte, 64)
		for {
			select {
			case <-chhbt:
				conn.Close()
				return
			default:
				conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				_, err := conn.Write([]byte("00"))
				if err != nil {
					log.Error("%s", err)
					conn.Close()
					reflag = true
					goto HBSTART
				}

				conn.SetReadDeadline(time.Now().Add(time.Second))
				_, err = conn.Read(respbuf)

				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (wk *Worker) Start(bindok chan byte) {
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
		fmt.Printf("listen [%s] error!\n", wk.Bind)
		bindok <- 0
		return
	}

	log.Error("Start Listen [%s] ok! server list [%s]", wk.Bind, strings.Join(wk.Servers, ","))

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

	go func(ch chan byte) {
		for {
			select {
			case ch <- 0:
				time.Sleep(100 * time.Millisecond)
			case <-time.After(5 * time.Second):
				log.Error("Worker corotine is clean over!")
				return
			}
		}
	}(wk.ChCtrl)

	go func(ch chan byte) {
		for {
			select {
			case ch <- 0:
			case <-time.After(5 * time.Second):
				log.Error("HeartBeat corotine is clean over!")
				return
			}
		}
	}(wk.ChHbtc)

	wk.ChCtrl = make(chan byte) // create new ChCtrl
	wk.ChHbtc = make(chan byte) // create new ChCtrl

	wk.Servers = servers[:len(servers)]
	log.Error("Reload Listen [%s] ok! server list [%s]", wk.Bind, strings.Join(wk.Servers, ","))

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
			wk.Index++
			newindex := wk.Index % len(wk.Servers)
			serv := wk.Servers[newindex]
			go wk.RealJob(conn, serv)
			// log.Info("[%s] no new client", wk.Name)
		}
	}
}
