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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/common"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestDestination_Read_databaseDoesNotExist(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dest := New()
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "random_id"},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"partKey": "partVal",
					"id":      "random_id",
				},
			},
		},
	})
	is.Equal(n, 0)
	is.True(strings.Contains(err.Error(), "Owner resource does not exist"))

	cancel()

	is.NoErr(dest.Teardown(context.Background()))
}

func TestDestination_Read_containerDoesNotExist(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	t.Cleanup(func() {
		dbCli, errNewDB := cli.NewDatabase(db.DatabaseProperties.ID)
		is.NoErr(errNewDB)

		_, err = dbCli.Delete(ctx, nil)
		is.NoErr(err)
	})

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "random_id"},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"partKey": "partVal",
					"id":      "random_id",
				},
			},
		},
	})
	is.Equal(n, 0)
	is.True(strings.Contains(err.Error(), "Resource Not Found"))

	cancel()

	is.NoErr(dest.Teardown(context.Background()))
}

func TestDestination_Write(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	containerCli, err := cli.NewContainer(cfg[config.KeyDatabase], cfg[config.KeyContainer])
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"id":      "random_id_1",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"id": "random_id_2",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  2,
					"field2":  "test_2",
					"field3":  false,
					"id":      "random_id_2",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationCreate,
			Key: sdk.StructuredData{
				"id": "random_id_3",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  3,
					"field2":  "test_3",
					"field3":  true,
					"id":      "random_id_3",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationCreate,
			Key: sdk.StructuredData{
				"id": "random_id_4",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  4,
					"field2":  "test_4",
					"field3":  false,
					"id":      "random_id_4",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"id": "random_id_2",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  20,
					"field2":  "test_20",
					"field3":  true,
					"id":      "random_id_2",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationDelete,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records[:3])
	is.Equal(n, 3)
	is.NoErr(err)

	is.NoErr(dest.Teardown(cctx))
	cancel()

	cctx, cancel = context.WithCancel(ctx)
	defer cancel()

	dest = Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err = dest.Write(cctx, records[3:])
	is.Equal(n, 3)
	is.NoErr(err)

	is.NoErr(dest.Teardown(cctx))
	cancel()

	queryPager := containerCli.NewQueryItemsPager("SELECT * FROM c ORDER BY c.field1 ASC OFFSET 0 LIMIT 10",
		azcosmos.NewPartitionKeyString("partVal"), nil)
	is.True(queryPager.More())

	queryResponse, err := queryPager.NextPage(ctx)
	is.NoErr(err)
	is.Equal(len(queryResponse.Items), 3)

	var (
		item           sdk.StructuredData
		metaProperties = []string{"_rid", "_ts", "_self", "_etag", "_attachments"}
	)

	// check the first item
	is.NoErr(json.Unmarshal(queryResponse.Items[0], &item))
	for i := range metaProperties {
		delete(item, metaProperties[i])
	}

	payload, err := json.Marshal(item)
	is.NoErr(err)
	is.Equal(records[2].Payload.After.Bytes(), payload)

	// check the second item
	is.NoErr(json.Unmarshal(queryResponse.Items[1], &item))
	for i := range metaProperties {
		delete(item, metaProperties[i])
	}

	payload, err = json.Marshal(item)
	is.NoErr(err)
	is.Equal(records[3].Payload.After.Bytes(), payload)

	// check the third item
	is.NoErr(json.Unmarshal(queryResponse.Items[2], &item))
	for i := range metaProperties {
		delete(item, metaProperties[i])
	}

	payload, err = json.Marshal(item)
	is.NoErr(err)
	is.Equal(records[4].Payload.After.Bytes(), payload)
}

func TestDestination_Write_successPopulatePayloadWithIDFromKey(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"partKey": "partVal",
				},
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records)
	is.Equal(n, 1)
	is.NoErr(err)

	is.NoErr(dest.Teardown(cctx))
	cancel()
}

func TestDestination_Write_successGetIDFromPayload(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"id":      "random_id_1",
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"partKey": "partVal",
				},
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records)
	is.Equal(n, 1)
	is.NoErr(err)

	is.NoErr(dest.Teardown(cctx))
	cancel()
}

func TestDestination_Write_failedKeyAndPayloadHaveNoID(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"notId": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"partKey": "partVal",
				},
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records)
	is.Equal(n, 0)
	is.Equal(err.Error(), "write record: route record: get id from sdk.Record.Key: "+
		"neither the sdk.Record.Key nor the sdk.Record.Payload.After contains the `id` key")

	is.NoErr(dest.Teardown(cctx))
	cancel()
}

func TestDestination_Write_failedCreateExistingItem(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"id":      "random_id_1",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationCreate,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"id":      "random_id_1",
					"partKey": "partVal",
				},
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records)
	is.Equal(n, 1)
	is.True(strings.Contains(err.Error(), "Entity with the specified id already exists in the system"))

	is.NoErr(dest.Teardown(cctx))
	cancel()
}

func TestDestination_Write_failedUpdateNonExistentItem(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
		ctx = context.Background()
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
	is.NoErr(err)

	dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
	is.NoErr(err)

	defer func() {
		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
	}()

	_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: cfg[config.KeyContainer],
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/partKey"},
		},
	}, nil)
	is.NoErr(err)

	records := []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Key: sdk.StructuredData{
				"id": "random_id_1",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  1,
					"field2":  "test_1",
					"field3":  true,
					"id":      "random_id_1",
					"partKey": "partVal",
				},
			},
		},
		{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"id": "random_id_3",
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"field1":  3,
					"field2":  "test_3",
					"field3":  true,
					"id":      "random_id_3",
					"partKey": "partVal",
				},
			},
		},
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dest := Destination{}
	is.NoErr(dest.Configure(cctx, cfg))
	is.NoErr(dest.Open(cctx))

	n, err := dest.Write(cctx, records)
	is.Equal(n, 1)
	is.True(strings.Contains(err.Error(), "Entity with the specified id does not exist in the system"))

	is.NoErr(dest.Teardown(cctx))
	cancel()
}

// prepareConfig gets the value of the environment variable named by the envNameURI and envNamePrimaryKey keys,
// generates names of database and container, and returns a configuration map.
func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	uri := os.Getenv(common.TestEnvNameURI)
	if uri == "" {
		t.Skipf("%s env var must be set", common.TestEnvNameURI)

		return nil
	}

	primaryKey := os.Getenv(common.TestEnvNamePrimaryKey)
	if primaryKey == "" {
		t.Skipf("%s env var must be set", common.TestEnvNamePrimaryKey)

		return nil
	}

	unixNano := time.Now().UnixNano()

	return map[string]string{
		config.KeyURI:            uri,
		config.KeyPrimaryKey:     primaryKey,
		config.KeyDatabase:       fmt.Sprintf("conduit_test_db_%d", unixNano),
		config.KeyContainer:      fmt.Sprintf("conduit_test_container_%d", unixNano),
		config.KeyPartitionValue: "partVal",
	}
}

func getClient(uri, primaryKey string) (*azcosmos.Client, error) {
	cred, err := azcosmos.NewKeyCredential(primaryKey)
	if err != nil {
		return nil, fmt.Errorf("new key credential: %w", err)
	}

	cli, err := azcosmos.NewClientWithKey(uri, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("new client with key: %w", err)
	}

	return cli, nil
}
