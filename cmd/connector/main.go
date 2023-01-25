package main

import (
	cosmosnosql "github.com/conduitio-labs/conduit-connector-cosmos-nosql"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(cosmosnosql.Connector)
}
