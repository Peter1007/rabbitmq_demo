package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	SERVER_HOST = "amqp://guest:guest@192.168.1.129:5672"
	ROUTE       = "rpc_queue"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randomString(l int) string {
	bytes := make([]byte, l)

	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}

	return string(bytes)
}

func fibonacciRpc(n int) (int, error) {
	var res int

	conn, err := amqp.Dial(SERVER_HOST)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return 0, err
	}

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		return 0, err
	}

	corrId := randomString(32)
	req := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrId,
		ReplyTo:       queue.Name,
		Body:          []byte(strconv.Itoa(n)),
	}
	err = ch.Publish("", ROUTE, false, false, req)
	if err != nil {
		return 0, err
	}

	for msg := range msgs {
		if corrId == msg.CorrelationId {
			res, err = strconv.Atoi(string(msg.Body))
			if err != nil {
				return 0, err
			}

			break
		}
	}

	return res, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s [n]\n", os.Args[0])
		os.Exit(-1)
	}

	rand.Seed(time.Now().UTC().UnixNano())

	n, err := strconv.Atoi(os.Args[1])
	failOnError(err, "Failed to convert arg to integer")

	log.Printf(" [x] Requesting fib(%d)\n", n)
	res, err := fibonacciRpc(n)
	failOnError(err, "Failed to handle RPC request")

	log.Printf(" [.] Got %d\n", res)
}
