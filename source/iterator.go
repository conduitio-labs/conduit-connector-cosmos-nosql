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

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
)

const (
	allColumns          = "*"
	table               = "c"
	limit               = "OFFSET 0 LIMIT %d"
	field               = "%s.%s"
	lessThanOrEqual     = "%s <= %s"
	greaterThan         = "%s > %s"
	lastProcessedParam  = "@lastProcessed"
	latestSnapshotParam = "@latestSnapshot"
)

// iterator is an implementation of an iterator for Azure Cosmos DB for NoSQL.
type iterator struct {
	position        *position
	containerClient *azcosmos.ContainerClient
	partitionKey    azcosmos.PartitionKey

	container   string
	keys        []string
	orderingKey string
	batchSize   uint

	items []sdk.StructuredData
}

// newIterator creates a new instance of the iterator.
func newIterator(ctx context.Context, config Config, sdkPosition sdk.Position) (*iterator, error) {
	iter := &iterator{
		partitionKey: azcosmos.NewPartitionKeyString(config.PartitionValue),
		container:    config.Container,
		keys:         config.Keys,
		orderingKey:  config.OrderingKey,
		batchSize:    config.BatchSize,
	}

	if len(iter.keys) == 0 {
		iter.keys = []string{iter.orderingKey}
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
	iter.containerClient, err = client.NewContainer(config.Database, config.Container)
	if err != nil {
		return nil, fmt.Errorf("new container client: %w", err)
	}

	iter.position, err = parsePosition(sdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	// it's a first launch of a pipeline
	if iter.position.LastProcessedValue == nil {
		// take the value of the orderingKey key of the most recent item in the container
		latestSnapshotValue, err := iter.latestSnapshotValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("get latest snapshot value: %w", err)
		}

		if config.Snapshot {
			// set this value to know where to stop returning records with [sdk.OperationSnapshot] operation
			iter.position.LatestSnapshotValue = latestSnapshotValue

			return iter, nil
		}

		// skip the snapshot, so set the most recent item in the container as the last processed item
		iter.position.LastProcessedValue = latestSnapshotValue
	}

	return iter, nil
}

// HasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *iterator) HasNext(ctx context.Context) (bool, error) {
	if iter.items != nil {
		return true, nil
	}

	err := iter.loadItems(ctx)
	if err != nil {
		return false, fmt.Errorf("load items: %w", err)
	}

	if iter.items != nil {
		return true, nil
	}

	if iter.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		iter.position.LastProcessedValue = iter.position.LatestSnapshotValue
		iter.position.LatestSnapshotValue = nil

		// and load new items
		err = iter.loadItems(ctx)
		if err != nil {
			return false, fmt.Errorf("load items: %w", err)
		}

		return iter.items != nil, nil
	}

	return false, nil
}

// Next returns the next record.
func (iter *iterator) Next() (sdk.Record, error) {
	key := make(sdk.StructuredData)
	for i := range iter.keys {
		val, ok := iter.items[0][iter.keys[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key %q not found", iter.keys[i])
		}

		key[iter.keys[i]] = val
	}

	rowBytes, err := json.Marshal(iter.items[0])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal item: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	pos := *iter.position
	// set the value from iter.orderingKey key you chose
	pos.LastProcessedValue = iter.items[0][iter.orderingKey]

	convertedPosition, err := pos.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position :%w", err)
	}

	iter.position = &pos

	metadata := sdk.Metadata{
		common.MetadataCosmosDBForNoSQLTable: iter.container,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	// remove the processed items from the slice
	iter.items = iter.items[1:]

	// if there are no items - set nil
	if len(iter.items) == 0 {
		iter.items = nil
	}

	if pos.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(convertedPosition, metadata, key, sdk.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(convertedPosition, metadata, key, sdk.RawData(rowBytes)), nil
}

// latestSnapshotValue returns the value of the orderingKey key
// of the most recent item in the container.
func (iter *iterator) latestSnapshotValue(ctx context.Context) (any, error) {
	query := sqlbuilder.NewSelectBuilder().
		Select(allColumns).
		From(table).
		OrderBy(fmt.Sprintf(field, table, iter.orderingKey)).Desc().
		SQL(fmt.Sprintf(limit, 1)).
		String()

	items, err := iter.selectItems(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("select latest snapshot value: %w", err)
	}

	if items != nil {
		return items[0][iter.orderingKey], nil
	}

	return nil, nil
}

// latestSnapshotValue returns the value of the orderingKey key of the most recent value item.
func (iter *iterator) loadItems(ctx context.Context) error {
	var queryParams []azcosmos.QueryParameter

	sb := sqlbuilder.NewSelectBuilder().
		Select(allColumns).
		From(table).
		OrderBy(fmt.Sprintf(field, table, iter.orderingKey)).Asc().
		SQL(fmt.Sprintf(limit, iter.batchSize))

	if iter.position.LastProcessedValue != nil {
		sb.Where(fmt.Sprintf(greaterThan, fmt.Sprintf(field, table, iter.orderingKey), lastProcessedParam))

		queryParams = append(queryParams, azcosmos.QueryParameter{
			Name:  lastProcessedParam,
			Value: iter.position.LastProcessedValue,
		})
	}

	if iter.position.LatestSnapshotValue != nil {
		sb.Where(fmt.Sprintf(lessThanOrEqual, fmt.Sprintf(field, table, iter.orderingKey), latestSnapshotParam))

		queryParams = append(queryParams, azcosmos.QueryParameter{
			Name:  latestSnapshotParam,
			Value: iter.position.LatestSnapshotValue,
		})
	}

	items, err := iter.selectItems(ctx, sb.String(), queryParams...)
	if err != nil {
		return fmt.Errorf("select items: %w", err)
	}

	// append new items to the end of the iter.items slice
	iter.items = append(iter.items, items...)

	return nil
}

// selectItems selects a batch of items by a query from a database.
func (iter *iterator) selectItems(
	ctx context.Context, query string, queryParameters ...azcosmos.QueryParameter,
) ([]sdk.StructuredData, error) {
	queryPager := iter.containerClient.NewQueryItemsPager(query, iter.partitionKey, &azcosmos.QueryOptions{
		QueryParameters: queryParameters,
	})

	if !queryPager.More() {
		return nil, nil
	}

	queryResponse, err := queryPager.NextPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("next page: %w", err)
	}

	if len(queryResponse.Items) == 0 {
		return nil, nil
	}

	items := make([]sdk.StructuredData, len(queryResponse.Items))
	for i := range queryResponse.Items {
		if err = json.Unmarshal(queryResponse.Items[i], &items[i]); err != nil {
			return nil, fmt.Errorf("unmarshal item: %w", err)
		}
	}

	return items, nil
}
