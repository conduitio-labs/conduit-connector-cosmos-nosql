package cosmosnosql

import sdk "github.com/conduitio/conduit-connector-sdk"

// Connector is a sdk.Connector of Azure Cosmos DB for NoSQL
var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        nil,
	NewDestination:   nil,
}
