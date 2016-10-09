package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

const (
	SERVER_HOST = "amqp://guest:guest@192.168.1.129:5672"
	QUEUE_NAME  = "rpc_queue"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func fib(n int) int {
	if n == 0 || n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}

func main() {
	conn, err := amqp.Dial(SERVER_HOST)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Faile to open a channel")
	defer ch.Close()

	queue, err := ch.QueueDeclare(QUEUE_NAME, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(1, 0, false)
	failOnError(err, "Failed to set Qos")

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	exit := make(chan bool)

	go func() {
		for msg := range msgs {
			n, err := strconv.Atoi(string(msg.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)\n", n)

			resp := amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: msg.CorrelationId,
				Body:          []byte(strconv.Itoa(fib(n))),
			}
			err = ch.Publish("", msg.ReplyTo, false, false, resp)
			failOnError(err, "Failed to publish a message")

			msg.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")

	<-exit
}
