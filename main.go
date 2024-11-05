package main

import (
	"context"
	"encoding/json"
	"fmt"
	c "goBroadcaster/constants"
	l "goBroadcaster/listnermanager"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var SendMessageChannel chan []byte

func consumer(lm *l.ListenerManager, timeout int, id string, ch chan c.Message, wg *sync.WaitGroup) {
	defer wg.Done()
	defer lm.RemoveListener(ch)

	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-ticker.C:
			log.Printf("Consumer %v timedout \n", id)
			return
		case msg := <-ch:
			if id == msg.Id {
				log.Printf("Consumer %v received message: %s\n", id, msg)
				go processMessage(id)
				return
			}
		}
	}
}

func processMessage(id string) {
	time.Sleep(1 * time.Millisecond)
	log.Printf("%v marked as done", id)
}

func main() {
	var wg sync.WaitGroup
	SendMessageChannel = make(chan []byte)
	lm := &l.ListenerManager{}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	tempQ, err := ch.QueueDeclare(
		"temp", // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to open a channel")

	recQ, err := ch.QueueDeclare(
		"krakenResponse", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(err, "Failed to open a channel")

	failOnError(err, "Failed to declare a queue")
	sendQ, err := ch.QueueDeclare(
		"SCBJOY80085-krakenAgent", // name
		true,                      // durable
		false,                     // delete when unused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		tempQ.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to open a channel")

	recQmsg, err := ch.Consume(
		recQ.Name, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to open a channel")

	wg.Add(3)
	go func() {
		defer wg.Done()
		log.Println("Started recQ")

		for d := range recQmsg {
			var message c.Message
			log.Printf("Received a message from agent: %s", d.Body)
			json.Unmarshal(d.Body, &message)
			wg.Add(1)
			lm.Broadcast(message)
		}
	}()

	go func() {
		defer wg.Done()
		for msg := range SendMessageChannel {
			log.Println("message")
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				err = ch.PublishWithContext(ctx,
					"",         // exchange
					sendQ.Name, // routing key
					false,      // mandatory
					false,      // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        msg,
					})
				if err != nil {
					log.Printf("Error: %s", err)
				}
				log.Printf("Sent %s\n", string(msg))
				lis := lm.AddListener()
				var message c.Message
				json.Unmarshal(msg, &message)
				log.Println(message)
				wg.Add(1)
				go consumer(lm, 2, message.Id, lis, &wg)

			}()
		}
		fmt.Println("bye")
	}()
	go func() {
		defer wg.Done()
		log.Println("Started tempQ")
		for d := range msgs {
			log.Println(d.Body)
			SendMessageChannel <- d.Body
			log.Println("message")

		}
	}()
	wg.Wait()
	failOnError(err, "Failed to register a consumer")
}
