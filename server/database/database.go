package database

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite" // sqlite
)

var (
	// DB variable shared across module
	DB *gorm.DB
)

// Init opens DB connection
func Init() {
	var err error
	DB, err = gorm.Open("sqlite3", "database.sqlite3")
	if err != nil {
		panic("failed to connect database")
	}
}

// Close closes DB connection
func Close() {
	DB.Close()
}
