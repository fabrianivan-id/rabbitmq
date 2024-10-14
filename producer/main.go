package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

// Configuration holds the RabbitMQ configuration parameters
type Configuration struct {
	RabbitMQURL      string
	ExchangeName     string
	RoutingKey       string
	NumProducers     int
	PublishFrequency time.Duration
}

// Producer represents a RabbitMQ producer
type Producer struct {
	id      int
	config  Configuration
	conn    *amqp.Connection
	channel *amqp.Channel
	wg      *sync.WaitGroup
}

// NewProducer creates a new Producer instance
func NewProducer(id int, config Configuration, wg *sync.WaitGroup) *Producer {
	return &Producer{
		id:     id,
		config: config,
		wg:     wg,
	}
}

// Connect establishes a connection and channel to RabbitMQ
func (p *Producer) Connect() error {
	var err error
	p.conn, err = amqp.Dial(p.config.RabbitMQURL)
	if err != nil {
		return err
	}

	p.channel, err = p.conn.Channel()
	if err != nil {
		return err
	}

	// Declare the exchange (idempotent)
	err = p.channel.ExchangeDeclare(
		p.config.ExchangeName, // name
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

	return nil
}

// Publish sends a message to the exchange with the routing key
func (p *Producer) Publish(message string) error {
	err := p.channel.Publish(
		p.config.ExchangeName, // exchange
		p.config.RoutingKey,   // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(message),
			DeliveryMode: amqp.Persistent,
		},
	)
	return err
}

// Start begins publishing messages at the specified frequency
func (p *Producer) Start(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.PublishFrequency)
	// defer ticker.Stop()

	// messageCount := 1

	for messageCount := 1; messageCount <= 10; messageCount++ {
		//for {
		select {
		case <-ctx.Done():
			log.Printf("Producer %d: Received shutdown signal", p.id)
			return
		case <-ticker.C:
			message := fmt.Sprintf("Producer %d: Message %d at %s", p.id, messageCount, time.Now().Format(time.RFC3339))
			err := p.Publish(message)
			if err != nil {
				log.Printf("Producer %d: Failed to publish message: %v", p.id, err)
			} else {
				log.Printf("Producer %d: Published message: %s", p.id, message)
			}
			//messageCount++
		}
	}
	ticker.Stop()
}

// Close gracefully shuts down the producer
func (p *Producer) Close() {
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			log.Printf("Producer %d: Failed to close channel: %v", p.id, err)
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			log.Printf("Producer %d: Failed to close connection: %v", p.id, err)
		}
	}
}

func main() {
	config := Configuration{
		RabbitMQURL:      getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		ExchangeName:     getEnv("EXCHANGE_NAME", "exchange_key"),
		RoutingKey:       getEnv("ROUTING_KEY", "my.routing.key"),
		NumProducers:     getEnvAsInt("NUM_PRODUCERS", 1),
		PublishFrequency: getEnvAsDuration("PUBLISH_FREQUENCY", 333*time.Millisecond),
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	producers := make([]*Producer, config.NumProducers)
	for i := 1; i <= config.NumProducers; i++ {
		producers[i-1] = NewProducer(i, config, &wg)
		if err := producers[i-1].Connect(); err != nil {
			log.Fatalf("Producer %d: Failed to connect to RabbitMQ: %v", i, err)
		}
		wg.Add(1)
		go producers[i-1].Start(ctx)
	}

	sig := <-sigs
	log.Printf("Received signal: %v. Initiating shutdown...", sig)

	cancel()
	wg.Wait()

	for _, producer := range producers {
		producer.Close()
	}

	log.Println("All producers have shut down gracefully")
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvAsInt(name string, defaultVal int) int {
	if valueStr, exists := os.LookupEnv(name); exists {
		var value int
		_, err := fmt.Sscanf(valueStr, "%d", &value)
		if err == nil {
			return value
		}
	}
	return defaultVal
}

func getEnvAsDuration(name string, defaultVal time.Duration) time.Duration {
	if valueStr, exists := os.LookupEnv(name); exists {
		if value, err := time.ParseDuration(valueStr); err == nil {
			return value
		}
	}
	return defaultVal
}
