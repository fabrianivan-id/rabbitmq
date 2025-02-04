// producer.go
package main

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Koneksi ke RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
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
	failOnError(err, "Failed to declare an exchange")

	// Pesan yang akan dikirim
	body := "Hello RabbitMQ!"

	// Publikasikan pesan ke exchange
	err = ch.Publish(
		"broadcast", // exchange
		"",          // routing key (tidak digunakan untuk fanout)
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // pesan persisten
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
