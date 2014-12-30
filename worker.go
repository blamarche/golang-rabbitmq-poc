/*
  Copyright 2014 Brendan LaMarche
  This file is part of golang-rabbitmq-poc.

  golang-rabbitmq-poc is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  golang-rabbitmq-poc is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with golang-rabbitmq-poc.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"fmt"
	"log"
	"time"
  "strconv"
  "os"
  
  "github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")
  
  eq, err := ch.QueueDeclare(
		"echo_request", // name
		false,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
  
  eqmsgs, err := ch.Consume(
		eq.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
  delay, _ := strconv.Atoi(os.Args[1])
  
	go func() {
		for d := range msgs {
			log.Printf("Processing ("+strconv.Itoa(delay)+") message: %s", d.Body)
			d.Ack(false)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}()
  
  go func() {
		for d := range eqmsgs {
			log.Printf("Echoing ("+strconv.Itoa(delay)+") request: %s", d.Body)
			err = ch.Publish(
        "",             // exchange
        d.ReplyTo,      // routing key
        false,          // mandatory
        false,
        amqp.Publishing{
          DeliveryMode: amqp.Persistent,
          ContentType:  "text/plain",
          Body:         []byte("[ECHO] "+string(d.Body)),
          CorrelationId: d.CorrelationId,
      })
      failOnError(err, "Failed to publish a response")
      
      d.Ack(false)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}()

	log.Printf(" [*] Waiting for messages and requests. To exit press CTRL+C")
	<-forever
}
