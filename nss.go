package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type ConfigJson struct {
	BindAddrs               []string
	BufferLen               int
	DialTimeoutMilliSeconds int64
	Servers                 []string
}

func copyForever(from, to *net.TCPConn, bufferLen int, ctx context.Context, cancelFunc context.CancelFunc) {
	buf := make([]byte, bufferLen)
	for {
		n, err := from.Read(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err != io.EOF {
				log.Printf("error reading from TCPConn (addr: %s, err: %v)", from.RemoteAddr(), err)
			}
			cancelFunc()

			err = from.Close()
			if err != nil {
				log.Printf("error closing src TCPConn (addr: %s, err: %v)", from.RemoteAddr(), err)
			}

			err = to.Close()
			if err != nil {
				log.Printf("error closing dst TCPConn (addr: %s, err: %v)", from.RemoteAddr(), err)
			}

			return
		}
		_, err = to.Write(buf[:n])
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err != io.EOF {
				log.Printf("error writing to TCPConn (addr: %s, err: %v)", to.RemoteAddr(), err)
			}
			cancelFunc()

			err = from.Close()
			if err != nil {
				log.Printf("error closing src TCPConn (addr: %s, err: %v)", from.RemoteAddr(), err)
			}

			err = to.Close()
			if err != nil {
				log.Printf("error closing dst TCPConn (addr: %s, err: %v)", from.RemoteAddr(), err)
			}

			return
		}
	}
}

func withMutex(m *sync.Mutex, f func()) {
	m.Lock()
	defer m.Unlock()
	f()
}

func main() {

	b, err := ioutil.ReadFile("config.json")
	if err != nil {
		log.Fatalf("error parsing config file (%v)", err)
	}

	configJson := ConfigJson{}
	err = json.Unmarshal(b, &configJson)
	if err != nil {
		log.Fatalf("error parsing config file as json (%v)", err)
	}

	serverIPs := make([]string, len(configJson.Servers))
	for i, serverIP := range configJson.Servers {
		ip := net.ParseIP(serverIP)
		if ip == nil {
			log.Fatalf("error parsing server IP %s", serverIP)
		}
		serverIPs[i] = serverIP
	}

	dialTimeout := time.Duration(configJson.DialTimeoutMilliSeconds) * time.Millisecond

	bufferLen := configJson.BufferLen
	bgctx := context.Background()

	for _, bindAddr := range configJson.BindAddrs {
		addr, err := net.ResolveTCPAddr("tcp", bindAddr)
		if err != nil {
			log.Fatalf("error parsing binding address (%s, %v)", bindAddr, err)
		}

		ln, err := net.ListenTCP("tcp", addr)
		if err != nil {
			log.Fatalf("failed to ListenTCP (%s, %v)", bindAddr, err)
		}

		log.Printf("listening on %s", bindAddr)

		port := addr.Port
		go func(ln *net.TCPListener) {
			// Create acceptor for each TCPListener

			for {
				conn, err := ln.AcceptTCP()
				if err != nil {
					log.Fatalf("failed to AcceptTCP (%s, %v)", bindAddr, err)
				}

				go func() {
					// Handle new connection

					ch := make(chan *net.TCPConn, 1)
					stopNewConns := false
					mutex := &sync.Mutex{}

					// Get the server with lowest latency
					for _, serverIP := range serverIPs {
						go func(serverIP string) {
							startTime := time.Now()
							serverAddr := serverIP + ":" + strconv.Itoa(port)
							remoteConn, err := net.DialTimeout("tcp", serverAddr, dialTimeout)

							withMutex(mutex, func() {
								if stopNewConns {
									if remoteConn != nil {
										err = remoteConn.Close()
										if err != nil {
											log.Printf("error closing remote TCPConn (addr: %s, err: %v)", remoteConn.RemoteAddr(), err)
										}
									}
									return
								} else {
									if err != nil {
										log.Printf("error establishing connection to remote (%s, %v)", serverAddr, err)
										return
									}

									log.Printf("successfully connected to remote %s (%d ms)", serverAddr, time.Since(startTime).Nanoseconds()/1000/1000)
									ch <- remoteConn.(*net.TCPConn)
									stopNewConns = true
								}
							})
						}(serverIP)
					}

					// Create pipeline
					remoteConn := <-ch
					pipeCtx, pipeCancel := context.WithCancel(bgctx)
					go copyForever(conn, remoteConn, bufferLen, pipeCtx, pipeCancel)
					go copyForever(remoteConn, conn, bufferLen, pipeCtx, pipeCancel)

				}()
			}
		}(ln)
	}

	<-bgctx.Done()

}
