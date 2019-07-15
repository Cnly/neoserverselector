package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"
)

// ConfigObject represents one config object specified in config
type ConfigObject struct {
	BindAddr                           string
	BufferLen                          int
	DialTimeoutMilliSeconds            int64
	MinimumAcceptedLatencyMilliseconds int64
	Servers                            []string
}

func copyForever(ctx context.Context, cancelFunc context.CancelFunc, from, to *net.TCPConn, bufferLen int) {
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

	var configObjects []ConfigObject
	err = json.Unmarshal(b, &configObjects)
	if err != nil {
		log.Fatalf("error parsing config file as json (%v)", err)
	}

	bgctx := context.Background()

	for _, configObject := range configObjects {
		addr, err := net.ResolveTCPAddr("tcp", configObject.BindAddr)
		if err != nil {
			log.Fatalf("error parsing binding address (%s, %v)", configObject.BindAddr, err)
		}

		ln, err := net.ListenTCP("tcp", addr)
		if err != nil {
			log.Fatalf("failed to ListenTCP (%s, %v)", configObject.BindAddr, err)
		}

		log.Printf("listening on %s", configObject.BindAddr)

		go func(ln *net.TCPListener, configObject ConfigObject) {
			// Create acceptor for each TCPListener

			dialTimeout := time.Duration(configObject.DialTimeoutMilliSeconds) * time.Millisecond

			bufferLen := configObject.BufferLen

			for {
				conn, err := ln.AcceptTCP()
				if err != nil {
					log.Fatalf("failed to AcceptTCP (%s, %v)", configObject.BindAddr, err)
				}

				go func() {
					// Handle new connection

					ch := make(chan *net.TCPConn, 1)
					failureChan := make(chan bool)
					stopNewConns := false
					mutex := &sync.Mutex{}

					// Get the server with lowest latency
					for _, serverAddr := range configObject.Servers {
						go func(serverAddr string) {
							startTime := time.Now()
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
								}

								if err != nil {
									log.Printf("error establishing connection to remote (%s, %v)", serverAddr, err)
									failureChan <- true
									return
								}

								latencyMS := time.Since(startTime).Nanoseconds() / 1000 / 1000
								if latencyMS < configObject.MinimumAcceptedLatencyMilliseconds {
									log.Printf("discarding connection to remote %s established in %d ms", serverAddr, latencyMS)
									failureChan <- true
									err = remoteConn.Close()
									if err != nil {
										log.Printf("error closing remote TCPConn (addr: %s, err: %v)", remoteConn.RemoteAddr(), err)
									}
									return
								}

								log.Printf("successfully connected to remote %s (%d ms)", serverAddr, latencyMS)
								close(failureChan)
								ch <- remoteConn.(*net.TCPConn)
								stopNewConns = true
							})
						}(serverAddr)
					}

					// Signal to close the local connection if all connection
					// attempts to remote servers failed
					go func() {
						for _ = range configObject.Servers {
							_, open := <-failureChan
							if !open {
								return
							}
						}
						log.Printf("error handling connection %s -> %s: all connection attempts to remote servers failed", conn.RemoteAddr(), configObject.BindAddr)
						close(ch)
					}()

					// Create pipeline
					remoteConn, open := <-ch
					if !open {
						log.Printf("closing connection %s -> %s: no remoteConn available", conn.RemoteAddr(), configObject.BindAddr)
						err := conn.Close()
						if err != nil {
							log.Printf("error closing connection %s -> %s: %v", remoteConn.RemoteAddr(), configObject.BindAddr, err)
						}
						return
					}

					pipeCtx, pipeCancel := context.WithCancel(bgctx)
					go copyForever(pipeCtx, pipeCancel, conn, remoteConn, bufferLen)
					go copyForever(pipeCtx, pipeCancel, remoteConn, conn, bufferLen)

				}()
			}
		}(ln, configObject)
	}

	<-bgctx.Done()

}
