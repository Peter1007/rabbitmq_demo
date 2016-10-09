package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func bodyFrom(args []string) string {
	var s string

	if len(args) < 2 || args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}

	return s
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@192.168.1.128:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare("logs", "fanout", true, false, false, false, nil)
	failOnError(err, "Failed to declear an exchange")

	for i := 0; i < 1000; i++ {
		body := bodyFrom(os.Args) + " " + strconv.Itoa(i+1)
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		}
		err = ch.Publish("logs", "", false, false, msg)
		failOnError(err, "Failed to publish a message")

		log.Printf(" [x] Sent %s\n", body)

		time.Sleep(10 * time.Millisecond)
	}
}
