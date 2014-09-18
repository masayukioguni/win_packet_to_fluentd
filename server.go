package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/masayukioguni/winformat"
	"github.com/t-k/fluent-logger-golang/fluent"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
)

const (
	defaultPort       = 9229
	defaultFluentHost = "127.0.0.1"
	defaultFluentPort = 24224
	defaultBufferSize = 1 * 1024 * 1024
	defaultTagName    = "debug.format"
)

type Payload struct {
	Addr         *net.UDPAddr
	Conn         *net.UDPConn
	Buffer       []byte
	BufferLength int
}

type Config struct {
	Port       int
	FluentHost string
	FluentPort int
	BufferSize int
	TagName    string
}

type Server struct {
	Config  Config
	conn    *net.UDPConn
	pending []byte
	mutex   sync.Mutex
}

func New(config Config) (s *Server, err error) {
	if config.Port == 0 {
		config.Port = defaultPort
	}
	if config.FluentHost == "" {
		config.FluentHost = defaultFluentHost
	}
	if config.FluentPort == 0 {
		config.FluentPort = defaultFluentPort
	}
	if config.TagName == "" {
		config.TagName = defaultTagName
	}
	if config.BufferSize == 0 {
		config.BufferSize = defaultBufferSize
	}
	s = &Server{Config: config}
	return s, err
}

func (s *Server) start() (err error) {
	config := &s.Config
	udpServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		log.Println("ResolveUDPAddr:", err)
		return err
	}

	s.conn, err = net.ListenUDP("udp", udpServerAddr)
	if err != nil {
		log.Println("ListenUDP:", err)
		return err
	}

	s.conn.SetReadBuffer(config.BufferSize)
	s.conn.SetWriteBuffer(config.BufferSize)

	payloadChannel := make(chan *Payload)

	go receivePayloadProcess(payloadChannel, s)
	go processPayload(payloadChannel, s)

	return err
}

func receivePayloadProcess(payloadChannel chan *Payload, s *Server) error {
	for {
		buffer := make([]byte, 1400)

		bufferLength, udpAddr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Failed udpConn.ReadFromUDP():", err)
			continue
		}
		currentPayload := &Payload{
			Addr:         udpAddr,
			Conn:         s.conn,
			Buffer:       buffer,
			BufferLength: bufferLength,
		}

		payloadChannel <- currentPayload
	}

	return nil
}

func processPayload(payloadChannel chan *Payload, s *Server) error {

	logger, err := fluent.New(fluent.Config{
		FluentPort: s.Config.FluentPort,
		FluentHost: s.Config.FluentHost})

	if err != nil {
		fmt.Println(err)
		return err
	}

	defer logger.Close()

	for {
		currentPayload := <-payloadChannel
		jsonData, _ := json.Marshal(winformat.NewWinFormat(currentPayload.Buffer))
		var data = map[string]string{
			"foo":  "bar",
			"hoge": "hoge"}
		fmt.Printf("%s\n", string(jsonData))
		fmt.Printf("%s\n", data)

		logger.Post(s.Config.TagName, winformat.NewWinFormat(currentPayload.Buffer))

	}
	return nil
}

// help text
const (
	help_text       string = `Usage: server [OPTIONS]`
	version_text    string = `server ` + Version
	port_text       string = ``
	fluentport_text string = ``
	fluenthost_text string = ``
	tagname_text    string = ``
	bufferSize_text string = ``
)

// need prefix?
var (
	help       = flag.Bool("help", false, help_text)
	version    = flag.Bool("version", false, version_text)
	port       = flag.Int("port", defaultPort, port_text)
	fluentPort = flag.Int("fluent_port", defaultFluentPort, fluentport_text)
	fluentHost = flag.String("fluent_host", defaultFluentHost, fluenthost_text)
	tagName    = flag.String("tag_name", defaultTagName, tagname_text)
	bufferSize = flag.Int("buffer_size", defaultBufferSize, bufferSize_text)
)

func main() {
	flag.Parse()
	if *help {
		fmt.Println(help_text)
		os.Exit(0)
	}
	if *version {
		fmt.Println(version_text)
		os.Exit(0)
	}

	server, _ := New(Config{
		Port:       *port,
		FluentPort: *fluentPort,
		FluentHost: *fluentHost,
		TagName:    *tagName,
		BufferSize: *bufferSize,
	})

	server.start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	_ = <-c
}
