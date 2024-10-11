package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

// Configuration holds the RabbitMQ configuration parameters
type Configuration struct {
	RabbitMQURL  string
	ExchangeName string
	QueueName    string
	RoutingKey   string
}

// Consumer represents a RabbitMQ consumer
type Consumer struct {
	config  Configuration
	conn    *amqp.Connection
	channel *amqp.Channel
	wg      sync.WaitGroup
}

// NewConsumer creates a new Consumer instance
func NewConsumer(config Configuration) *Consumer {
	return &Consumer{config: config}
}

// Connect establishes a connection and channel to RabbitMQ
func (c *Consumer) Connect() error {
	var err error
	c.conn, err = amqp.Dial(c.config.RabbitMQURL)
	if err != nil {
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}

	// Declare the exchange
	err = c.channel.ExchangeDeclare(
		c.config.ExchangeName, // name
		"direct",              // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return err
	}

	// Declare the queue
	_, err = c.channel.QueueDeclare(
		c.config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	// Bind the queue to the exchange with the routing key
	err = c.channel.QueueBind(
		c.config.QueueName,    // queue name
		c.config.RoutingKey,   // routing key
		c.config.ExchangeName, // exchange
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return err
	}

	return nil
}

// StartConsuming begins consuming messages from the queue
func (c *Consumer) StartConsuming(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		c.config.QueueName, // queue
		"consumer2",        // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					log.Println("Message channel closed")
					return
				}
				c.handleMessage(msg)
			case <-ctx.Done():
				log.Println("Context canceled, stopping consumer")
				return
			}
		}
	}()
	return nil
}

// handleMessage processes a single message
func (c *Consumer) handleMessage(msg amqp.Delivery) {
	defer msg.Ack(false)
	log.Printf("Received message: %s", string(msg.Body))
}

// Close gracefully shuts down the consumer
func (c *Consumer) Close() {
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			log.Printf("Failed to close channel: %v", err)
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}
}

func main() {
	config := Configuration{
		RabbitMQURL:  getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		ExchangeName: getEnv("EXCHANGE_NAME", "exchange_key"),
		QueueName:    getEnv("QUEUE_NAME", "queue_group_1"),
		RoutingKey:   getEnv("ROUTING_KEY", "my.routing.key"),
	}

	consumer := NewConsumer(config)

	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	if err := consumer.StartConsuming(ctx); err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	sig := <-sigs
	log.Printf("Received signal: %v. Initiating shutdown...", sig)

	cancel()
	consumer.wg.Wait()
	consumer.Close()
	log.Println("Consumer shut down gracefully")
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
