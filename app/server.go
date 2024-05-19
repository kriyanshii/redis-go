package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pingCommand      = "PING"
	echoCommand      = "ECHO"
	setCommand       = "SET"
	getCommand       = "GET"
	pingResponse     = "+PONG\r\n"
	okResponse       = "+OK\r\n"
	notFoundResponse = "$-1\r\n"
)

type Store struct {
	Data     map[string]string
	Expiries map[string]time.Time
	Mutex    sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		Data:     make(map[string]string),
		Expiries: make(map[string]time.Time),
	}
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.Data[key] = value
	if ttl > 0 {
		s.Expiries[key] = time.Now().Add(ttl)
	} else {
		delete(s.Expiries, key)
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if expiry, exists := s.Expiries[key]; exists && time.Now().After(expiry) {
		delete(s.Data, key)
		delete(s.Expiries, key)
		return "", false
	}
	val, ok := s.Data[key]
	return val, ok
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	store := NewStore()

	// Uncomment this block to pass the first stage

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		// to listen to multiple ping's from same user.
		go handleConnection(connection, store)
	}
}

func handleConnection(connection net.Conn, store *Store) {
	defer connection.Close()
	// smallest tcp packet
	buff := make([]byte, 1024)
	for {
		n, err := connection.Read(buff)
		if err != nil || n == 0 {
			return
		}
		commands := parse(buff[:n])
		if len(commands) == 0 {
			continue
		}

		switch commands[0] {
		case "echo":
			connection.Write([]byte(createResponseMsg(commands[1])))
		case "ping":
			connection.Write([]byte(pingResponse))
		case "set":
			if len(commands) >= 3 {
				ttl := time.Duration(0)
				if len(commands) == 5 && commands[3] == "px" {
					if parsedTTL, err := strconv.Atoi(commands[4]); err == nil {
						ttl = time.Duration(parsedTTL) * time.Millisecond
					}
				}
				store.Set(commands[1], commands[2], ttl)
				connection.Write([]byte(okResponse))
			}
		case "get":
			val, ok := store.Get(commands[1])
			if !ok {
				connection.Write([]byte(notFoundResponse))
			} else {
				connection.Write([]byte(createResponseMsg(val)))
			}
		}
	}
}

func createResponseMsg(msg string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
}

func parse(input []byte) []string {
	rawInput := string(input)
	commands := strings.Split(rawInput, "\r\n")
	var parsedCommands []string
	if strings.HasPrefix(commands[0], "*") {
		_, err := strconv.Atoi(commands[0][1:])
		if err != nil {
			return []string{"Encountered error"}
		}
		checkLengthFlag := false
		for _, v := range commands[1:] {
			if strings.HasPrefix(v, "$") {
				_, err := strconv.Atoi(v[1:])
				if err != nil {
					return []string{"Encountered error while parsing $"}
				}
				checkLengthFlag = true
			} else if checkLengthFlag {
				checkLengthFlag = false
				parsedCommands = append(parsedCommands, strings.ToLower(v))
			}
		}
	}
	return parsedCommands
}
