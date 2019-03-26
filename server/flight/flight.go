package flight

import (
	"github.com/felixputera/cz4013-flight-info/server/database"
	// "github.com/jinzhu/gorm"
	"errors"
	"time"
)

// Flight type
type Flight struct {
	ID            string `gorm:"primary_key"`
	From          string
	To            string
	Time          time.Time
	AvailabeSeats int
	Fare          int
}

func Init() {
	database.DB.AutoMigrate(&Flight{})
}

func FindFlights(from, to string) []Flight {
	var flights []Flight
	database.DB.Find(&flights, Flight{From: from, To: to})
	return flights
}

func GetFlight(id string) (Flight, error) {
	var flight Flight
	database.DB.Find(&flight, Flight{ID: id})
	if flight == (Flight{}) {
		return flight, errors.New("Flight not found")
	}
	return flight, nil
}

// MakeReservation makes flight reservation and reduce the number of available seats
func MakeReservation(id string, numTickets int) error {
	flight, err := GetFlight(id)
	if err != nil {
		return err
	}
	if flight.AvailabeSeats < numTickets {
		return errors.New("Flight doesn't have enough available seats")
	}
	flight.AvailabeSeats -= numTickets
	database.DB.Save(&flight)
	return nil
}

func NewFlight(
	id,
	from,
	to string,
	time time.Time,
	availableSeats,
	fare int) (Flight, error) {

	if f, _ := GetFlight(id); f != (Flight{}) {
		return Flight{}, errors.New("Duplicate flight number found")
	}

	flight := Flight{
		ID:            id,
		From:          from,
		To:            to,
		Time:          time,
		AvailabeSeats: availableSeats,
		Fare:          fare,
	}
	database.DB.Create(&flight)

	return flight, nil
}
