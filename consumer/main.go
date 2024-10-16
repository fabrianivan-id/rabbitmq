// consumer.go
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func startConsumer(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Koneksi ke RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to connect to RabbitMQ", id))
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to open a channel", id))
	defer ch.Close()

	// Deklarasikan exchange bertipe 'fanout'
	err = ch.ExchangeDeclare(
		"broadcast", // nama exchange
		"fanout",    // tipe
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to declare an exchange", id))

	// Deklarasikan queue unik untuk setiap consumer
	queueName := fmt.Sprintf("consumer_queue_%d", id)
	q, err := ch.QueueDeclare(
		queueName, // nama queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to declare a queue", id))

	// Bind queue ke exchange
	err = ch.QueueBind(
		q.Name,      // nama queue
		"",          // routing key
		"broadcast", // nama exchange
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to bind a queue", id))

	// Konsumsi pesan
	msgs, err := ch.Consume(
		q.Name, // nama queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, fmt.Sprintf("Consumer %d: Failed to register a consumer", id))

	log.Printf("Consumer %d: Waiting for messages. To exit press CTRL+C", id)

	for d := range msgs {
		log.Printf("Consumer %d: Received a message: %s", id, d.Body)
	}
}

func main() {
	var wg sync.WaitGroup

	// Daftar consumer yang akan dijalankan

	wg.Add(1)
	go startConsumer(1, &wg)

	wg.Add(1)
	go startConsumer(2, &wg)

	wg.Add(1)
	go startConsumer(3, &wg)

	wg.Add(1)
	go startConsumer(4, &wg)

	wg.Wait()
}
