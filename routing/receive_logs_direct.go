package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
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

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s [info] [warm] [error]\n", os.Args[0])
		os.Exit(-1)
	}

	conn, err := amqp.Dial(SERVER_HOST)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, false, false, nil)
	failOnError(err, "Failed to declear an exchange")

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declear a queue")

	for _, s := range os.Args[1:] {
		log.Printf("Binding queue %s to exchange %s with routing key %s", queue.Name, EXCHANGE_NAME, s)
		err = ch.QueueBind(queue.Name, s, EXCHANGE_NAME, false, nil)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	exit := make(chan bool)

	go func() {
		for msg := range msgs {
			log.Printf("Received [x] %s\n", msg.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL-C\n")
	<-exit
}
