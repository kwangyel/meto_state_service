package main

import (
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SeatState struct {
	SeatId       int
	ScheduleHash string
	BookingId    int
	Status       string
	gorm.Model
}

type queryDTO struct {
	bookingId    int
	scheduleHash string
	status       string
	msgType      string
	seatId       int
	id           int
}
type DB struct {
	create        chan queryDTO
	updatePaid    chan queryDTO
	updateBooking chan queryDTO
	delete        chan queryDTO
	checkTime     chan bool
}

func newDB() *DB {
	return &DB{
		create:        make(chan queryDTO),
		updatePaid:    make(chan queryDTO),
		updateBooking: make(chan queryDTO),
		delete:        make(chan queryDTO),
		checkTime:     make(chan bool),
	}
}

func (d *DB) run(removeList chan *[]SeatState) {
	dsn := "app:*meto@2021#@tcp(127.0.0.1:3306)/meto_state?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println("error connection to database")
	} else {
		log.Printf("Connected to database")
	}
	db.AutoMigrate(&SeatState{})

	for {
		select {
		case query := <-d.checkTime:
			log.Println("CheckTimer: ", query)
			var seats []SeatState
			var toRemove []SeatState
			db.Where("status = ?", STATUS_UNPAID).Find(&seats)

			for _, item := range seats {
				objectTime := item.CreatedAt
				currentTime := time.Now()
				diff := currentTime.Sub(objectTime)
				log.Println(diff)
				if diff > EXP_UNIT*EXP_TIME {
					// toUpdateIndex = append(toUpdateIndex, int(item.ID))
					toRemove = append(toRemove, item)
				}
			}
			// log.Println(toUpdateIndex)
			if len(toRemove) > 0 {
				removeList <- &toRemove
			}
		case query := <-d.create:
			// log.Println("creating the entry %v", query)
			db.Create(&SeatState{ScheduleHash: query.scheduleHash, SeatId: query.seatId, Status: STATUS_UNPAID})
		case query := <-d.updatePaid:
			var seat SeatState
			//find the seat first
			db.First(&seat, "schedule_hash = ? AND seat_id = ?", query.scheduleHash, query.seatId)
			db.Model(&seat).Updates(map[string]interface{}{"Status": STATUS_PAID})
		case query := <-d.updateBooking:
			var seat SeatState
			db.First(&seat, "schedule_hash = ? AND seat_id = ?", query.scheduleHash, query.seatId)
			db.Model(&seat).Updates(map[string]interface{}{"booking_id": query.bookingId})
		case query := <-d.delete:
			db.Delete(&SeatState{}, "schedule_hash= ? AND seat_id = ?", query.scheduleHash, query.seatId)
		}

	}
}
