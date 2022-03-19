# Rustle

Rustle is a simple, in-memory broker written in pure Golang. Design has been inspired by **Redis Streams**.

## Features

- It allows multiple clients to cooperate through the **Consumer Group** concept
- In-memory streams allow to overcome the limitations of a log files
- Clean and intuitive RESTful APIs make it suitable for the web

## Introduction to Rustle

In Rustle, streams are primarily an append only data structure, abstracting a continuous flow of message entries. A stream entry a json-object containing a "data" field and some additional infos.

You can easily create a new stream by runnning the following curl command:

```bash
foo@bar:~$ curl -X PUT -i localhost:8080/streams/myStream
```

where "myStream" is the name of the stream to be created. Upon success, the command will output an HTTP 201 response.

To start listening from the created stream, type the following command in a new terminal window:

```bash
foo@bar:~$ curl localhost:8080/streams/myStream/messages
```

This will create an SSE subscription which can be used to listen for each new incoming message. To verify that all is working properly, let us push some messages to the new stream.

```bash
foo@bar:~$ for i in {1..10}; do curl -X POST -i localhost:8080/streams/myStream \ 
-d "\"Hello, this is a test message\""; done;
```

You should see a sequence of ten json objects arriving from the active subscription with a format like this:

```bash
{"id": ..., "timestamp": ...,"stream": "myStream","data": "Hello, this is a test message"}
```

As you can see, each message reports the **timestamp** related to the instant the message has been received by the server, a random generated **uuid**, and the **name** of the stream the message has been submitted to. The actual content of the message is instead stored in the **data** field.

## Contribute

The software is still at early stages. Any contribution, in the form of a suggestion, bug report or pull request, can be useful and is well accepted :blush:
