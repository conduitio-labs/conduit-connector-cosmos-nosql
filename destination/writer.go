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

package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var errNoID = errors.New("neither the sdk.Record.Key nor the sdk.Record.Payload.After contains the `id` key")

// Writer implements a writer logic for the Neo4j Destination.
type writer struct {
	containerClient *azcosmos.ContainerClient
	partitionKey    azcosmos.PartitionKey
}

// New creates a new instance of the [Writer].
func newWriter(config Config) (*writer, error) {
	w := &writer{
		partitionKey: azcosmos.NewPartitionKeyString(config.PartitionValue),
	}

	// create an KeyCredential containing the account's primary key
	cred, err := azcosmos.NewKeyCredential(config.PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("new key credential: %w", err)
	}

	// create a new instance of Cosmos client
	client, err := azcosmos.NewClientWithKey(config.URI, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	// create a new instance of the specific container client
	w.containerClient, err = client.NewContainer(config.Database, config.Container)
	if err != nil {
		return nil, fmt.Errorf("new container client: %w", err)
	}

	return w, nil
}

// Write writes a record to the destination.
func (w *writer) Write(ctx context.Context, record sdk.Record) error {
	err := sdk.Util.Destination.Route(ctx, record,
		w.create,
		w.replace,
		w.delete,
		w.create,
	)
	if err != nil {
		return fmt.Errorf("route record: %w", err)
	}

	return nil
}

func (w *writer) create(ctx context.Context, record sdk.Record) error {
	id, err := w.getID(&record)
	if err != nil {
		return fmt.Errorf("get id from sdk.Record.Key: %w", err)
	}

	err = w.populatePayloadWithID(&record, id)
	if err != nil {
		return fmt.Errorf("populate payload with %q key: %w", common.KeyID, err)
	}

	_, err = w.containerClient.CreateItem(ctx, w.partitionKey, record.Payload.After.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("create item: %w", err)
	}

	return nil
}

func (w *writer) replace(ctx context.Context, record sdk.Record) error {
	id, err := w.getID(&record)
	if err != nil {
		return fmt.Errorf("get id from sdk.Record.Key: %w", err)
	}

	err = w.populatePayloadWithID(&record, id)
	if err != nil {
		return fmt.Errorf("populate payload with %q key: %w", common.KeyID, err)
	}

	_, err = w.containerClient.ReplaceItem(ctx, w.partitionKey, id, record.Payload.After.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("replace item: %w", err)
	}

	return nil
}

func (w *writer) delete(ctx context.Context, record sdk.Record) error {
	id, err := w.getID(&record)
	if err != nil {
		return fmt.Errorf("get id from sdk.Record.Key: %w", err)
	}

	_, err = w.containerClient.DeleteItem(ctx, w.partitionKey, id, nil)
	if err != nil {
		return fmt.Errorf("delete item: %w", err)
	}

	return nil
}

// getID returns a value by `id` key from the sdk.Record.Key
// or from the sdk.Record.Payload.After.
func (w *writer) getID(record *sdk.Record) (string, error) {
	if record.Key != nil {
		key := make(sdk.StructuredData)
		if err := json.Unmarshal(record.Key.Bytes(), &key); err != nil {
			return "", fmt.Errorf("unmarshal sdk.Record.Key: %w", err)
		}

		if id, ok := key[common.KeyID]; ok {
			return fmt.Sprintf("%v", id), nil
		}
	}

	if record.Payload.After != nil {
		payload := make(sdk.StructuredData)
		if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
			return "", fmt.Errorf("unmarshal payload: %w", err)
		}

		if id, ok := payload[common.KeyID]; ok {
			return fmt.Sprintf("%v", id), nil
		}
	}

	return "", errNoID
}

// populatePayloadWithID populates the sdk.Record.Payload with id key
// from the sdk.Record.Key, if it not exists.
func (w *writer) populatePayloadWithID(record *sdk.Record, id string) error {
	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	if _, ok := payload[common.KeyID]; ok {
		return nil
	}

	//nolint:forcetypeassert // there is no point in checking the type because it was unmarshalled above
	record.Payload.After.(sdk.StructuredData)[common.KeyID] = id

	return nil
}
