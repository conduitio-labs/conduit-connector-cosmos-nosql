package cosmosnosql

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "cosmos-nosql",
		Summary: "Azure Cosmos DB for NoSQL destination plugin for Conduit, written in Go.",
		Description: "Azure Cosmos DB for NoSQL connector is one of Conduit plugins. " +
			"It provides both, a Source and a Destination Azure Cosmos DB for NoSQL connectors.",
		Version: version,
		Author:  "Meroxa, Inc. & Yalantis",
	}
}
