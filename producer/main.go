package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

func main() {
	// memulai koneksi dengan rabbitmq
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat terkoneksi ke AMQP", err)
	}
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat membuat sebuah amqpChannel", err)
	}
	defer amqpChannel.Close()

	// mendeklarasikan antrian
	queue, err := amqpChannel.QueueDeclare("add", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("%s: %s", "Tidak dapat mendeklarasi antrian `add`", err)
	}

	limit := 10
	var wg sync.WaitGroup
	wg.Add(limit)

	for i := 0; i < limit; i++ {
		number := i
		go func() {
			defer wg.Done()
			err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("Data %v", number)),
			})

			if err != nil {
				log.Fatalf("Kesalahan saat mengirimkan pesan: %s", err)
			}
		}()
	}

	wg.Wait()

	fmt.Println("selesai")
}
