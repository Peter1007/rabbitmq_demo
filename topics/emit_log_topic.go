package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"strconv"
	"time"
)

const (
	SERVER_HOST   = "amqp://guest:guest@192.168.1.128:5672"
	EXCHANGE_NAME = "logs_topic"
	EXCHANGE_TYPE = "topic"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func serverityFrom() string {
	serverities := []string{"order.get_list", "order.get_info", "order.update", "ayi.add", "ayi.get_list", "ayi.get_list"}

	return serverities[rand.Intn(len(serverities))]
}

func main() {
	conn, err := amqp.Dial(SERVER_HOST)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, false, false, nil)
	failOnError(err, "Failed to declare an ecchange")

	for i := 0; i < 100; i++ {
		serverity := serverityFrom()
		body := serverity + " " + strconv.Itoa(i+1)
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		}
		err = ch.Publish(EXCHANGE_NAME, serverity, false, false, msg)
		failOnError(err, "Failed to publish a message")

		log.Printf("I send %s\n", body)

		time.Sleep(10 * time.Millisecond)
	}

}
