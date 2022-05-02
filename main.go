package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
)

const (
	ON_LOCK            = "ON_LOCK"
	ON_BOOK            = "ON_BOOK"
	ON_LOCK_LEAVE      = "LOCK_LEAVE"
	ON_LOCK_CONFIRM    = "LOCK_CONFIRM"
	ON_LOCK_CANCEL     = "LOCK_CANCEL"
	ON_BOOKING_CREATED = "ON_BOOKING_CREATED"

	CREATE = "CREATE"
	UPDATE = "UPDATE"
	DELETE = "DELETE"
	GET    = "GET"

	STATUS_PAID   = "PAID"
	STATUS_UNPAID = "UNPAID"

	TICKER_TIME = 1 //minutes to fire the check
	TICKER_UNIT = time.Minute

	// expiry time
	EXP_TIME = 7
	EXP_UNIT = time.Minute
)

type MsgDTO struct {
	ScheduleHash string `json:"scheduleHash"`
	MessageType  string `json:"messageType"`
	SeatId       string `json:"seatId"`
	BookingId    int    `json:"bookingId"`
}

var addr = flag.String("addr", ":9090", "http service address")

func main() {
	flag.Parse()
	log.Println("Running state service")

	//gets the removelist channel when there is items to remove
	removeList := make(chan *[]SeatState)

	//db thread
	dbManager := newDB()
	go dbManager.run(removeList)

	go timerThread(dbManager)

	go amqpThread(removeList, dbManager)

	router := gin.New()

	router.GET("/state-service/", func(c *gin.Context) {
		log.Println("health pinged")
		c.JSON(200, "OK")
	})

	router.Run("0.0.0.0" + *addr)

}

func timerThread(dbm *DB) {
	ticker := time.NewTicker(TICKER_TIME * TICKER_UNIT)
	for {
		select {
		case t := <-ticker.C:
			fmt.Println("Time at", t)
			// c <- true
			dbm.checkTime <- true
		}
	}

}

func amqpThread(remoteList chan *[]SeatState, dbm *DB) {
	amqpServerUrl := "amqp://guest:guest@localhost:5672/"
	connectMq, err := amqp.Dial(amqpServerUrl)
	if err != nil {
		panic(err)
	}
	defer connectMq.Close()

	channelMq, err := connectMq.Channel()
	if err != nil {
		panic(err)
	}
	defer channelMq.Close()
	err = channelMq.ExchangeDeclare(
		"meto",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
	}

	q, err := channelMq.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
	}

	err = channelMq.QueueBind(
		q.Name,
		"",
		"meto",
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
	}

	messages, err := channelMq.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	log.Println("Connected to MQ server")

	// DB connection

	//consumer thread
	go func() {
		for message := range messages {
			log.Printf(" > Received message: %s\n", message.Body)
			mqMsg := &MsgDTO{}
			err := json.Unmarshal([]byte(message.Body), mqMsg)
			if err != nil {
				log.Println(err)
				continue
			}
			switch msgType := mqMsg.MessageType; msgType {
			case ON_BOOKING_CREATED:
				seatId, err := strconv.Atoi(mqMsg.SeatId)
				if err != nil {
					log.Println(err)
					continue
				}
				dbm.updateBooking <- queryDTO{scheduleHash: mqMsg.ScheduleHash, bookingId: mqMsg.BookingId, seatId: seatId}
			case ON_BOOK:
				seatId, err := strconv.Atoi(mqMsg.SeatId)
				if err != nil {
					log.Println(err)
					continue
				}
				dbm.updatePaid <- queryDTO{scheduleHash: mqMsg.ScheduleHash, seatId: seatId}
			case ON_LOCK_CANCEL:
				seatId, err := strconv.Atoi(mqMsg.SeatId)
				if err != nil {
					log.Println(err)
					continue
				}
				dbm.delete <- queryDTO{scheduleHash: mqMsg.ScheduleHash, seatId: seatId}

			case ON_LOCK_CONFIRM:
				seatId, err := strconv.Atoi(mqMsg.SeatId)
				if err != nil {
					log.Println(err)
					continue
				}
				dbm.create <- queryDTO{scheduleHash: mqMsg.ScheduleHash, seatId: seatId, status: STATUS_UNPAID}
				// dbm.get <- queryDTO{scheduleId: scheduleId, seatId: seatId, status: STATUS_UNPAID}
			}
		}
	}()

	for {
		select {
		case mqmsg := <-remoteList:
			for _, item := range *mqmsg {
				seatId := strconv.Itoa(item.SeatId)
				dbm.delete <- queryDTO{scheduleHash: item.ScheduleHash, id: int(item.ID), seatId: item.SeatId}
				b, err := json.Marshal(MsgDTO{ScheduleHash: item.ScheduleHash, MessageType: ON_LOCK_CANCEL, SeatId: seatId})
				if err != nil {
					panic(err)
				}

				postBody, _ := json.Marshal(map[string]string{
					"scheduleHash": item.ScheduleHash,
					"seatNumber":   seatId,
					"token":        "123",
				})

				responseBody := bytes.NewBuffer(postBody)
				resp, err := http.Post("http://127.0.0.1:3000/bookings/timeout", "application/json", responseBody)
				if err != nil {
					log.Printf("[Error] Http post request error %v ", err)
				}
				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("[Error] Http post request error %v ", err)
				}
				sb := string(body)
				log.Printf("[Debug] Timeout http request. %v", sb)

				channelMq.Publish("meto", "", false, false, amqp.Publishing{
					DeliveryMode: 2,
					ContentType:  "text/plain",
					Body:         b,
				})
			}
		}
	}

}
