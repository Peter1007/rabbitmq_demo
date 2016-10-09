package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare("logs", "fanout", true, false, false, false, nil)
	failOnError(err, "Failed to declear an ecchange")

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declear a queue")

	err = ch.QueueBind(queue.Name, "", "logs", false, nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	exit := make(chan bool)

	go func() {
		for msg := range msgs {
			log.Printf(" [x] Received %s\n", msg.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL-C")
	<-exit
}
