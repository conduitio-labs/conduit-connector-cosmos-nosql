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
	"errors"
	"testing"

	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/destination/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestination_Configure_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Destination{}

	var (
		raw = map[string]string{
			config.KeyURI:            "https://localhost:8081",
			config.KeyPrimaryKey:     "C2y6yDjf5",
			config.KeyDatabase:       "database_id",
			config.KeyContainer:      "container_it",
			config.KeyPartitionValue: "partVal_test",
		}
		want = Config{
			Config: config.Config{
				URI:            "https://localhost:8081",
				PrimaryKey:     "C2y6yDjf5",
				Database:       "database_id",
				Container:      "container_it",
				PartitionValue: "partVal_test",
			},
		}
	)

	err := s.Configure(context.Background(), raw)
	is.NoErr(err)

	is.Equal(s.config, want)
}

func TestDestination_Write_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockWriter(ctrl)
	it.EXPECT().Write(ctx, sdk.Record{}).Return(nil)

	d := Destination{writer: it}

	records, err := d.Write(ctx, []sdk.Record{{}})
	is.NoErr(err)
	is.Equal(records, 1)
}

func TestDestination_Write_failInsertRecord(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockWriter(ctrl)
	it.EXPECT().Write(ctx, sdk.Record{}).Return(errors.New("insert record: fail"))

	d := Destination{writer: it}

	_, err := d.Write(ctx, []sdk.Record{{}})
	is.True(err != nil)
}
