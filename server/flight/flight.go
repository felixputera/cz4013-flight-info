package flight

import (
	"errors"
	"time"

	"github.com/felixputera/cz4013-flight-info/server/database"
)

// Flight type
type Flight struct {
	ID            string `gorm:"primary_key"`
	From          string `gorm:"index"`
	To            string `gorm:"index"`
	Time          string
	AvailabeSeats int32
	Fare          float32
}

func Init() {
	database.DB.AutoMigrate(&Flight{})
}

func FindFlightIDsFromTo(from, to string) ([]string, error) {
	var flights []*Flight
	var flightIDs []string

	database.DB.Find(&flights, Flight{From: from, To: to})
	if len(flights) == 0 {
		return flightIDs, errors.New("no flight found")
	}

	for _, flight := range flights {
		flightIDs = append(flightIDs, flight.ID)
	}

	return flightIDs, nil
}

func GetFlight(id string) (*Flight, error) {
	if id == "" {
		return nil, errors.New("flight not found")
	}
	var flight *Flight
	flight = new(Flight)
	database.DB.Find(flight, Flight{ID: id})
	if *flight == (Flight{}) {
		return nil, errors.New("flight not found")
	}
	return flight, nil
}

// MakeReservation makes flight reservation and reduce the number of available seats
func MakeReservation(id string, seats int32) error {
	flight, err := GetFlight(id)
	if err != nil {
		return err
	}
	if flight.AvailabeSeats < seats {
		return errors.New("flight doesn't have enough available seats")
	}
	flight.AvailabeSeats -= seats
	database.DB.Save(flight)
	return nil
}

func NewFlight(
	id,
	from,
	to,
	time string,
	availableSeats int32,
	fare float32) (*Flight, error) {

	if f, _ := GetFlight(id); f != nil {
		return nil, errors.New("duplicate flight number found")
	}

	flight := &Flight{
		ID:            id,
		From:          from,
		To:            to,
		Time:          time,
		AvailabeSeats: availableSeats,
		Fare:          fare,
	}
	database.DB.Create(flight)

	return flight, nil
}

func MonitorAvailableSeats(id string, durationMs int32) (<-chan int32, <-chan error) {
	resChan := make(chan int32)
	errChan := make(chan error)
	var prevAvailableSeats int32

	go func() {
		defer close(resChan)
		defer close(errChan)

		timeout := time.After(time.Duration(durationMs) * time.Millisecond)
		ticker := time.Tick(500 * time.Millisecond)

		query := func() {
			flight, err := GetFlight(id)
			if err != nil {
				errChan <- err
			} else if flight.AvailabeSeats != prevAvailableSeats {
				resChan <- flight.AvailabeSeats
				prevAvailableSeats = flight.AvailabeSeats
			}
		}

		query()

		for {
			select {
			case <-timeout:
				return
			case <-ticker:
				query()
			}
		}
	}()

	return resChan, errChan
}

func FindDestinationsFrom(from string) ([]string, error) {
	var destinationSet = make(map[string]bool)
	var destinations []string
	var flights []*Flight

	database.DB.Find(&flights, Flight{From: from})
	if len(flights) == 0 {
		return destinations, errors.New("no flight found")
	}

	for _, flight := range flights {
		destinationSet[flight.To] = true
	}
	for from := range destinationSet {
		destinations = append(destinations, from)
	}

	return destinations, nil
}
