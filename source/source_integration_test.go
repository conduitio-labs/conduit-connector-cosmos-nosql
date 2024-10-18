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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/common"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Read_databaseDoesNotExist(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "key1", "true")
		ctx = context.Background()
	)

	src := New()

	err := src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.True(strings.Contains(err.Error(), "Owner resource does not exist"))
}

func TestSource_Read_containerDoesNotExist(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "orderingKey", "true")
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

	src := New()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.True(strings.Contains(err.Error(), "Resource Not Found"))
}

func TestSource_Read_containerHasNoData(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "orderingKey", "true")
	)

	cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			Paths: []string{fmt.Sprintf("/%s", cfg[config.KeyPartitionValue])},
		},
	}, nil)
	is.NoErr(err)

	src := New()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_combinedIterator(t *testing.T) {
	var (
		ctx = context.Background()
		is  = is.New(t)
		cfg = prepareConfig(t, "key2", "true", "key1", "key2")
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

	// prepare container with two items
	// insert the first item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"3794cb1a","partKey":"partVal","key1":"1","key2":1}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	// insert the second item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"ed053fb6","partKey":"partVal","key1":"2","key2":2}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	src := New()

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = src.Configure(cctx, cfg)
	is.NoErr(err)

	err = src.Open(cctx, nil)
	is.NoErr(err)

	record, err := src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "1",
		"key2": float64(1),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":1,"latestSnapshotValue":2}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"id":"3794cb1a","key1":"1","key2":1,"partKey":"partVal"}`))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	// insert the third item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"2452d9a6","partKey":"partVal","key1":"3","key2":3}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	src = New()

	cctx, cancel = context.WithCancel(ctx)
	defer cancel()

	err = src.Configure(cctx, cfg)
	is.NoErr(err)

	err = src.Open(cctx, record.Position)
	is.NoErr(err)

	record, err = src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "2",
		"key2": float64(2),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":2,"latestSnapshotValue":2}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"id":"ed053fb6","key1":"2","key2":2,"partKey":"partVal"}`))

	record, err = src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "3",
		"key2": float64(3),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":3,"latestSnapshotValue":null}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"id":"2452d9a6","key1":"3","key2":3,"partKey":"partVal"}`))

	_, err = src.Read(cctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert the forth item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"d0e7c1af","partKey":"partVal","key1":"4","key2":4}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	record, err = src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "4",
		"key2": float64(4),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":4,"latestSnapshotValue":null}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"id":"d0e7c1af","key1":"4","key2":4,"partKey":"partVal"}`))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_snapshotIsFalse(t *testing.T) {
	var (
		ctx = context.Background()
		is  = is.New(t)
		cfg = prepareConfig(t, "key2", "false", "key1", "key2")
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

	// prepare container with two items
	// insert the first item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"3794cb1a","partKey":"partVal","key1":"1","key2":1}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	// insert the second item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"ed053fb6","partKey":"partVal","key1":"2","key2":2}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	src := New()

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = src.Configure(cctx, cfg)
	is.NoErr(err)

	err = src.Open(cctx, nil)
	is.NoErr(err)

	_, err = src.Read(cctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert the third item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"2452d9a6","partKey":"partVal","key1":"3","key2":3}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	record, err := src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "3",
		"key2": float64(3),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":3,"latestSnapshotValue":null}`))

	_, err = src.Read(cctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert the forth item
	_, err = containerCli.CreateItem(ctx,
		azcosmos.NewPartitionKeyString(cfg[config.KeyPartitionValue]),
		[]byte(`{"id":"d0e7c1af","partKey":"partVal","key1":"4","key2":4}`),
		&azcosmos.ItemOptions{
			ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
		})
	is.NoErr(err)

	record, err = src.Read(cctx)
	is.NoErr(err)
	is.Equal(record.Key, opencdc.StructuredData(map[string]any{
		"key1": "4",
		"key2": float64(4),
	}))
	is.Equal(record.Position, opencdc.Position(`{"lastProcessedValue":4,"latestSnapshotValue":null}`))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

// prepareConfig gets the value of the environment variable named by the envNameURI and envNamePrimaryKey keys,
// generates names of database and container, and returns a configuration map.
func prepareConfig(t *testing.T, orderingKey, snapshot string, keys ...string) map[string]string {
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
		ConfigKeyOrderingKey:     orderingKey,
		ConfigKeyKeys:            strings.Join(keys, ","),
		ConfigKeySnapshot:        snapshot,
		ConfigKeyMetaProperties:  "false",
		ConfigKeyBatchSize:       "100",
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
