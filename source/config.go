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

package source

import "github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"

const (
	// ConfigKeyOrderingKey is a config name for a orderingKey field.
	ConfigKeyOrderingKey = "orderingKey"
	// ConfigKeySnapshot is a config name for a snapshot field.
	ConfigKeySnapshot = "snapshot"
	// ConfigKeyBatchSize is a config name for a batchSize field.
	ConfigKeyBatchSize = "batchSize"
)

// Config holds configurable values specific to source.
type Config struct {
	config.Config

	// The name of a key that is used for ordering items.
	OrderingKey string `json:"orderingKey" validate:"required"`
	// Determines whether the connector will take a snapshot
	// of all items before starting CDC mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// The size of an element batch.
	BatchSize uint `json:"batchSize" validate:"gt=0,lt=100001" default:"1000"`
}
