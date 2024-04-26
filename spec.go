// Copyright © 2023 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cosmosnosql

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process (i.e. the Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

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
