package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkerFunc is a function type for worker functions.
type WorkerFunc func(int, amqp.Delivery)

// dispatcher fetches messages from the queue and dispatches them to workers.
func dispatcher(ch *amqp.Channel, queueName string, dispatchCh chan amqp.Delivery, stopCh chan struct{}) {
	// Register a consumer to fetch messages from the queue.
	tasks, err := ch.Consume(
		queueName, // Queue name
		"",        // Consumer name (optional)
		false,     // Auto-Ack (acknowledging tasks manually)
		false,     // Exclusive
		false,     // No-local
		false,     // No-Wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				// The channel is closed, indicating no more tasks.
				return
			}
			dispatchCh <- task // Place the message in the dispatch channel.
		case <-stopCh:
			return
		}
	}
}

// workerFunc is an example worker function.
func workerFunc(id int, task amqp.Delivery) {
	// Example worker function - replace with custom processing logic.
	timetosleep := rand.Intn(10)
	

	time.Sleep(time.Duration(timetosleep) * time.Second) // Simulated processing time
	now := time.Now()
	fmt.Printf("Worker %d: Processed task from %s: %s in %d seconds done at: %v: %v : %v \n", id, task.ConsumerTag, task.Body, timetosleep, now.Hour(), now.Minute(), now.Second())

	// Acknowledge the message to tell RabbitMQ it's been processed.
	if err := task.Ack(false); err != nil {
		log.Printf("Worker %d: Failed to acknowledge the message: %v", id, err)
	}
}

var globalConnection *amqp.Connection

// InitializeQueueConsumer sets up message processing.
func InitializeQueueConsumer(queueName string, numWorkers int, workerFn WorkerFunc, parallelism int) {
	conn := globalConnection

	// Open a channel on the RabbitMQ connection.
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	stopCh := make(chan struct{})                       // Stop channel for graceful shutdown.
	dispatchCh := make(chan amqp.Delivery, parallelism) // Buffered channel for message distribution.
	workerWg := &sync.WaitGroup{}                       // WaitGroup to wait for worker goroutines.

	// Start the dispatcher to fetch messages and dispatch them to the workers.
	go dispatcher(ch, queueName, dispatchCh, stopCh)

	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			for {
				select {
				case task, ok := <-dispatchCh:
					if !ok {
						// No more tasks to process.
						return
					}
					workerFn(workerID, task) // Process the message using the worker function.
				case <-stopCh:
					return
				}
			}
		}(i)
	}

	fmt.Printf("Initialized %d workers for queue: %s. Press Ctrl+C to exit.\n", numWorkers, queueName)
	workerWg.Wait()   // Wait for all worker goroutines to finish.
	close(stopCh)     // Signal workers and dispatcher to stop.
	close(dispatchCh) // Close the dispatch channel.
}

func main() {
	// Set your RabbitMQ connection URL here.
	rabbitMQURL := "amqp://guest:guest@localhost:5672/"

	if err := godotenv.Load(); err != nil {
		fmt.Print("env not loaded")
		os.Exit(0)
	}

	// Connect to RabbitMQ.
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	globalConnection = conn

	// Read configuration from environment variables.
	queueName := os.Getenv("QUEUE_NAME")
	numWorkersStr := os.Getenv("NUM_WORKERS")
	parallelismStr := os.Getenv("PARALLELISM")

	if queueName == "" || numWorkersStr == "" || parallelismStr == "" {
		log.Fatal("Environment variables not set.")
	}

	numWorkers, err := strconv.Atoi(numWorkersStr)
	parallelism, err := strconv.Atoi(parallelismStr)

	// Initialize message processing.
	InitializeQueueConsumer(queueName, numWorkers, workerFunc, parallelism)
}
