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

package config

const (
	// KeyURI is the config name for an uri field.
	KeyURI = "uri"
	// KeyPrimaryKey is the config name for a primaryKey field.
	KeyPrimaryKey = "primaryKey"
	// KeyDatabase is the config name for a database field.
	KeyDatabase = "database"
	// KeyContainer is the config name for a container field.
	KeyContainer = "container"
	// KeyPartitionValue is the config name for a partitionValue field.
	KeyPartitionValue = "partitionValue"
	// KeyKeys is the config name for a keys field.
	KeyKeys = "keys"
)

// Config contains common source and destination configurable values.
type Config struct {
	// The connection uri pointed to an Azure Cosmos DB for NoSQL instance.
	URI string `json:"uri" validate:"required"`
	// The key for authentication with Azure Cosmos DB.
	PrimaryKey string `json:"primaryKey" validate:"required"`
	// The name of a database the connector should work with.
	Database string `json:"database" validate:"required"`
	// The name of a container the connector should work with.
	Container string `json:"container" validate:"required"`
	// The logical partition key value.
	PartitionValue string `json:"partitionValue" validate:"required"`
	// Comma-separated list of key names to build the sdk.Record.Key.
	Keys []string `json:"keys"`
}
