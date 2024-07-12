# Server

The `server` package contains the code responsible for spinning up the TCP server and handling connections to it.
It also handles passing different requests to the appropriate command handler and providing it with the useful context (`server.Context`) which includes things like accessing the [RESP](../resp/README.md) parser and the [database](../storage/README.md).
