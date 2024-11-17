package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pingCommand      = "PING"
	pingMessage      = "*1\r\n$4\r\nPING\r\n"
	echoCommand      = "ECHO"
	setCommand       = "SET"
	getCommand       = "GET"
	pingResponse     = "+PONG\r\n"
	okResponse       = "+OK\r\n"
	notFoundResponse = "$-1\r\n"
)

var replicaOf = flag.String("replicaof", "", "Replicate to another server")
var emptyRDB, _ = hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
var slaves = []net.Conn{}

type Store struct {
	Data     map[string]string
	Expiries map[string]time.Time
	Mutex    sync.RWMutex
}

type Replica struct {
	offset int
}

var replica = &Replica{}

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

	port := flag.Int("port", 6379, "The port which the redis server listens")
	flag.Parse()

	if *replicaOf != "" {
		log.Print("approached")
		go replicateMaster(*replicaOf, store)
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*port))
	if err != nil {
		fmt.Printf("Failed to bind to port %v", port)
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
		go handleConnection(connection, store, false)
	}
}

func handleConnection(connection net.Conn, store *Store, isMaster bool) {
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
			if !isMaster {
				connection.Write([]byte(createResponseMsg(commands[1])))
			}
		case "ping":
			if !isMaster {
				connection.Write([]byte(pingResponse))
			}
		case "set":
			if len(commands) >= 3 {
				ttl := time.Duration(0)
				if len(commands) == 5 && commands[3] == "px" {
					if parsedTTL, err := strconv.Atoi(commands[4]); err == nil {
						ttl = time.Duration(parsedTTL) * time.Millisecond
					}
				}
				for _, slave := range slaves {
					slave.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(commands[1]), commands[1], len(commands[2]), commands[2])))
				}
				store.Set(commands[1], commands[2], ttl)
				if !isMaster {
					connection.Write([]byte(okResponse))
				}
			}
		case "get":
			val, ok := store.Get(commands[1])
			if !ok {
				if !isMaster {
					connection.Write([]byte(notFoundResponse))
				}
			} else {
				if !isMaster {
					connection.Write([]byte(createResponseMsg(val)))
				}
			}
		case "info":
			infoResponse := "role:master"
			infoResponse += fmt.Sprintf("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n")
			infoResponse += fmt.Sprintf("master_repl_offset:0\r\n")
			if *replicaOf != "" {
				infoResponse = "role:slave"
			}
			if !isMaster {
				connection.Write([]byte(createResponseMsg(infoResponse)))
			}
		case "replconf":
			if !isMaster {
				connection.Write([]byte(okResponse))
			}
		case "psync":
			slaves = append(slaves, connection)
			if !isMaster {
				connection.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
				connection.Write(append([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))), emptyRDB...))
			}
		}
	}
}

func replicateMaster(address string, store *Store) {
	parts := strings.Split(address, " ")
	if len(parts) != 2 {
		fmt.Println("Invalid master address format. Expected <MASTER_HOST> <MASTER_PORT>")
	}
	masterHost := parts[0]
	masterPort := parts[1]
	masterConn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		fmt.Printf("failed to connect to master at %s:%s\n", masterHost, masterPort)
	}
	defer masterConn.Close()

	_, err = masterConn.Write([]byte(pingMessage))
	if err != nil {
		fmt.Println("Failed to send PING to master: ", err)
		os.Exit(1)
	}

	time.Sleep(1 * time.Second)
	masterConn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	time.Sleep(1 * time.Second)
	masterConn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	time.Sleep(1 * time.Second)
	masterConn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))

	buff := make([]byte, 1024)
	for {
		n, err := masterConn.Read(buff)
		if err != nil || n == 0 {
			return
		}
		commands := parse(buff[:n])
		if len(commands) == 0 {
			continue
		}
		switch commands[0] {
		case "set":
			if len(commands) >= 3 {
				ttl := time.Duration(0)
				if len(commands) == 5 && commands[3] == "px" {
					if parsedTTL, err := strconv.Atoi(commands[4]); err == nil {
						ttl = time.Duration(parsedTTL) * time.Millisecond
					}
				}
				store.Set(commands[1], commands[2], ttl)
				replica.offset += n
			}
		case "replconf":
			len := len(strconv.Itoa(replica.offset))
			masterConn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len, replica.offset)))
			log.Print("offset: ", n, string(buff))
			replica.offset += n
		default:
			replica.offset += n
			log.Print("offset: ", n, string(buff))
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
