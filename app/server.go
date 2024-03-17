package main

import (
	"fmt"
	"strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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
		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) {
	defer connection.Close()
	// smalles tcp packet
	buff := make([]byte, 1024)
	for {
		n, err := connection.Read(buff)
		if err != nil || n == 0 {
			return
		}
		commands := parse(buff)
		if commands[0] == "echo" {
			connection.Write([]byte(fmt.Sprintf("+%v\r\n", commands[1])))
		} else if commands[0] == "ping" {
			connection.Write([]byte("+PONG\r\n"))
		}
	}
}

func parse(input []byte) []string {
	rawInput := string(input)
	commands := strings.Split(rawInput, "\r\n")
	var parsedCommands []string
	if strings.HasPrefix(commands[0], "*") {
		_, err := strconv.Atoi(commands[0][1:])
		if err != nil {
			return []string{"Encounterd error"}
		}
		checkLengthFlag := false
		for _, v := range commands[1:] {
			if strings.HasPrefix(v, "$") {
				_, err := strconv.Atoi(v[1:])
				if err != nil {
					return []string{"Encounterd error while parsing $"}
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
