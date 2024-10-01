package main

import (
	"log"
	"os"

	"github.com/streadway/amqp"
)

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// memulai koneksi dengan rabbitmq
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat terkoneksi ke AMQP", err)
	}
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat terkoneksi ke amqpChannel", err)
	}
	defer amqpChannel.Close()

	// mendeklarasikan antrian
	queue, err := amqpChannel.QueueDeclare("add", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat mendeklarasikan antrian `add`", err)
	}

	err = amqpChannel.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat mengkonfigurasi QoS", err)
	}

	autoAck, exclusive, noLocal, noWait := false, false, false, false

	// Mengkonsumsi pesan dari antrian
	messageChannel, err := amqpChannel.Consume(
		queue.Name,
		"",
		autoAck,
		exclusive,
		noLocal,
		noWait,
		nil,
	)
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat mendaftarkan konsumen", err)
	}

	stopChan := make(chan bool)

	// Mulai mengkonsumsi pesan
	go func() {
		log.Printf("Konsumen siap, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Pesan diterima: %s", d.Body)

			if err := d.Ack(false); err != nil {
				log.Printf("Kesalahan saat mengakui pesan: %s", err)
			} else {
				log.Printf("Pesan diakui")
			}
		}
	}()

	// Berhenti untuk menghentikan program
	<-stopChan
}
