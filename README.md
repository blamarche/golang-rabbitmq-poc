golang-rabbitmq-poc
===================

Simple RabbitMQ implementation example to handle both process tasks and request/reply functions.


Usage:

go run tasker.go (milliseconds-to-sleep)

go run requester.go (milliseconds-to-sleep)

go run worker.go (milliseconds-to-sleep)


Run as many as desired (with a RabbitMQ server on localhost).

License: GPLv3
