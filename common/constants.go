// Copyright © 2022 Meroxa, Inc. & Yalantis
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

package common

const (
	// KeyID is the mandatory key for each database item.
	KeyID = "id"
	// TestEnvNameURI is a key for connection uri pointed to an Azure Cosmos DB for NoSQL instance.
	TestEnvNameURI = "COSMOS_NOSQL_URI"
	// TestEnvNamePrimaryKey is a key for authentication with Azure Cosmos DB.
	TestEnvNamePrimaryKey = "COSMOS_NOSQL_PRIMARY_KEY"
)
