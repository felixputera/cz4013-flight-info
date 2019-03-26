package main

import (
	"fmt"
	"github.com/felixputera/cz4013-flight-info/server/database"
	"github.com/felixputera/cz4013-flight-info/server/flight"
	"time"
)

func main() {
	database.Init()
	flight.Init()
	defer database.Close()

	_, err := flight.NewFlight("sq60", "singapore", "hongkong", time.Now(), 100, 100)
	if err != nil {
		fmt.Println(err)
	}

	f, err := flight.GetFlight("sq60")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(f.ID)
}
