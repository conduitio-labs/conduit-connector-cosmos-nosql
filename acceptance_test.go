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
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/common"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	orderInt int64
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt64(&d.orderInt, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata:  map[string]string{},
		Key: sdk.StructuredData{
			"id": fmt.Sprintf("%d", d.orderInt),
		},
		Payload: sdk.Change{After: sdk.RawData(
			fmt.Sprintf(`{"col1":%d,"id":"%d","partKey":"partVal"}`, d.orderInt, d.orderInt),
		)},
	}
}

func TestAcceptance(t *testing.T) {
	uri := os.Getenv(common.TestEnvNameURI)
	if uri == "" {
		t.Skipf("%s env var must be set", common.TestEnvNameURI)
	}

	primaryKey := os.Getenv(common.TestEnvNamePrimaryKey)
	if primaryKey == "" {
		t.Skipf("%s env var must be set", common.TestEnvNamePrimaryKey)
	}

	unixNano := time.Now().UnixNano()

	cfg := map[string]string{
		config.KeyURI:                  uri,
		config.KeyPrimaryKey:           primaryKey,
		config.KeyDatabase:             fmt.Sprintf("conduit_test_db_%d", unixNano),
		config.KeyContainer:            fmt.Sprintf("conduit_test_container_%d", unixNano),
		config.KeyPartitionValue:       "partVal",
		source.ConfigKeyOrderingKey:    "col1",
		source.ConfigKeySnapshot:       "true",
		source.ConfigKeyMetaProperties: "false",
		source.ConfigKeyBatchSize:      "100",
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(cfg),
				AfterTest:         afterTest(cfg),
				GoleakOptions: []goleak.Option{
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
			},
		},
	})
}

// beforeTest creates the test table.
func beforeTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
		is.NoErr(err)

		ctx := context.Background()

		db, err := cli.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg[config.KeyDatabase]}, nil)
		is.NoErr(err)

		dbCli, err := cli.NewDatabase(db.DatabaseProperties.ID)
		is.NoErr(err)

		_, err = dbCli.CreateContainer(ctx, azcosmos.ContainerProperties{
			ID: cfg[config.KeyContainer],
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths: []string{"/partKey"},
			},
		}, nil)
		is.NoErr(err)
	}
}

// afterTest drops the test table.
func afterTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		cli, err := getClient(cfg[config.KeyURI], cfg[config.KeyPrimaryKey])
		is.NoErr(err)

		dbCli, err := cli.NewDatabase(cfg[config.KeyDatabase])
		is.NoErr(err)

		_, err = dbCli.Delete(context.Background(), nil)
		is.NoErr(err)
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
