package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

const (
	SERVER_HOST   = "amqp://guest:guest@192.168.1.128:5672"
	EXCHANGE_NAME = "logs_direct"
	EXCHANGE_TYPE = "direct"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func bodyFrom(serverity string) string {
	messages := map[string]string{"info": "info message", "warm": "warm message", "error": "error message"}
	return messages[serverity]
}

func serverityFrom() string {
	serverities := []string{"info", "warm", "error"}

	return serverities[rand.Intn(3)]
}

func main() {
	rand.Seed(time.Now().UnixNano())

	conn, err := amqp.Dial(SERVER_HOST)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, false, false, nil)
	failOnError(err, "Failed to declear a change")

	for i := 0; i < 100; i++ {
		serverity := serverityFrom()
		body := bodyFrom(serverity)
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		}
		err = ch.Publish(EXCHANGE_NAME, serverity, false, false, msg)
		failOnError(err, "Failed to publish a message")

		log.Printf(" [x] Sent %s\n", body)

		time.Sleep(10 * time.Millisecond)
	}
}
