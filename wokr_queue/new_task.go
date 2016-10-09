package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"os"
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
	rand.Seed(time.Now().UnixNano())

	conn, err := amqp.Dial("amqp://guest:guest@192.168.1.129:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	queue, err := ch.QueueDeclare("task_queue", true, false, false, false, nil)
	failOnError(err, "Failed to declear a queue")

	for i := 0; i < 10000; i++ {
		body := bodyFrom(os.Args)

		dotNum := rand.Intn(3)
		for dotNum > 0 {
			body = body + "."
			dotNum--
		}

		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		}
		err = ch.Publish("", queue.Name, false, false, msg)
		failOnError(err, "Failed to publish a message")
		log.Printf(" [x] Sent %s\n", body)

		time.Sleep(time.Millisecond * 100)
	}
}
