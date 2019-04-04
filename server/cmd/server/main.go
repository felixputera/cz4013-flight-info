package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/felixputera/cz4013-flight-info/server/database"
	"github.com/felixputera/cz4013-flight-info/server/flight"
	"github.com/felixputera/cz4013-flight-info/server/rpc"
)

func main() {
	var filterDuplicate bool
	var port int

	flag.BoolVar(&filterDuplicate, "filter", false, "filter duplicate request")
	flag.IntVar(&port, "port", 12345, "server UDP listen port")

	flag.Parse()

	database.Init()
	flight.Init()
	defer database.Close()

	transport, _ := rpc.NewServerUDPSocket(fmt.Sprintf(":%d", port), filterDuplicate)
	processor := flight.NewProcessor()
	server := rpc.NewUdpServer(processor,
		transport,
		rpc.NewTransportFactory(),
		rpc.NewBinaryProtocolFactory(),
	)

	log.Printf("Starting server on port %d\n", port)
	log.Println("Filtering duplicate:", filterDuplicate)
	server.Serve()
}
