# Redis in Go

A simple Redis server implementation in Go.

The implementation is divided into multiple packages, each has a directory in the root of the project with a README file that explains what the package does.

## Building

To build the project, run:

```sh
go build -o redis-server .
```

## Running

To run the server you can execute the binary that was built in the previous step:

```sh
./redis-server
```

You can configure the server by passing different flags, to view the available flags run:

```sh
./redis-server -h
```
