# gRPC bi-directional streams example 

This project features a gRPC server which hosts a single bidirectional
streams based service. The service has a single method, connect().

Clients establish a connection to this service, and then call connect(),
at which point the server replies with an "ok" message.

Immediately after the server sends the "ok" message, it sends a ServerStreamRequest.
The message includes a random number, which indicates the number of items
the client should stream back to the server.

The client first responds with a StreamBegin message. It then begins
streaming back StreamItems, up to the number received in the ServerStreamRequest.
Once finished, the client sends a StreamEnd message, indicating that the
entire ServerStreamRequest call was successful.

Currently, the number of connect() calls made is 20, and all of this is
done using a single client channel.

## Build

`./gradlew jar`

## Starting the server

`java -jar build/libs/grpc-bidi.jar`

## Starting the client

`java -cp build/libs/grpc-bidi.jar foo.bar.Client`