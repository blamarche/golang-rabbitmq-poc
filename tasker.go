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
	"os"
  "strconv"
  "time"
  "math/rand"

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

	body := "Task "
  i := 0
  
  delay, _ := strconv.Atoi(os.Args[1])
  
  for {
    k := int(rand.Float32()*4+1)
    
    for m:=0; m<k; m++ {
        
      newbody := body+ strconv.Itoa(i)
      
      err = ch.Publish(
        "",           // exchange
        "task_queue", // routing key
        false,        // mandatory
        false,
        amqp.Publishing{
          DeliveryMode: amqp.Persistent,
          ContentType:  "text/plain",
          Body:         []byte(newbody),
      })
      failOnError(err, "Failed to publish a message")
      
      i++;
    }
    
    log.Println("Added tasks: "+strconv.Itoa(k))
    
    time.Sleep(time.Duration(delay) * time.Millisecond)
  }
}

