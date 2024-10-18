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
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/config"
	"github.com/conduitio-labs/conduit-connector-cosmos-nosql/source/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestSource_Configure_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	var (
		raw = map[string]string{
			config.KeyURI:            "https://localhost:8081",
			config.KeyPrimaryKey:     "C2y6yDjf5",
			config.KeyDatabase:       "database_id",
			config.KeyContainer:      "container_it",
			config.KeyPartitionValue: "partVal_test",
			ConfigKeyOrderingKey:     "id",
			ConfigKeyKeys:            "name,created_at",
			ConfigKeySnapshot:        "false",
			ConfigKeyMetaProperties:  "true",
			ConfigKeyBatchSize:       "5000",
		}
		want = Config{
			Config: config.Config{
				URI:            "https://localhost:8081",
				PrimaryKey:     "C2y6yDjf5",
				Database:       "database_id",
				Container:      "container_it",
				PartitionValue: "partVal_test",
			},
			OrderingKey:    "id",
			Keys:           []string{"name", "created_at"},
			Snapshot:       false,
			MetaProperties: true,
			BatchSize:      5000,
		}
	)

	err := s.Configure(context.Background(), raw)
	is.NoErr(err)

	is.Equal(s.config, want)
}

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	key := make(opencdc.StructuredData)
	key["field1"] = 1

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Time{})

	record := opencdc.Record{
		Position: opencdc.Position(`{"lastProcessedValue": 1}`),
		Metadata: metadata,
		Key:      key,
		Payload: opencdc.Change{
			After: key,
		},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next().Return(record, nil)

	s := Source{iterator: it}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_failHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("load items: some error"))

	s := Source{iterator: it}

	_, err := s.Read(ctx)
	is.True(err != nil)
}

func TestSource_Read_failNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next().Return(opencdc.Record{}, errors.New("marshal item: some error"))

	s := Source{iterator: it}

	_, err := s.Read(ctx)
	is.True(err != nil)
}
