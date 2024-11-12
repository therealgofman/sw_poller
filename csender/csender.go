package csender

import (
	"fmt"
	log "github.com/inconshreveable/log15"
	"net"
)

type Sender struct {
	Hosts []string
	Socks map[int]net.Conn
	Log   log.Logger
}

func New(hosts []string, logger log.Logger) *Sender {
	s := new(Sender)
	s.Socks = make(map[int]net.Conn)
	s.Log = logger

	i := 0
	for _, host := range hosts {
		conn, err := net.Dial("udp", host)
		if err != nil {
			s.Log.Crit("Error dialing: %v", err)
			panic(err)
		}

		i++
		s.Socks[i] = conn
	}

	return s
}

func (s *Sender) Listen(strings <-chan string) {
	curSock := 1
	maxSock := len(s.Socks)
	for {
		msg := <-strings
		fmt.Printf("Sending string: '%s' to socket #%d\n", msg, curSock)
		//fmt.Fprintf(s.Socks[curSock], msg)

		if curSock == maxSock {
			curSock = 1
		} else {
			curSock++
		}
	}
}
