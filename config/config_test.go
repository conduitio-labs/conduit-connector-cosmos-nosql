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

import (
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestConfig_Parse_success(t *testing.T) {
	t.Parallel()

	var (
		raw = map[string]string{
			KeyURI:            "https://localhost:8081",
			KeyPrimaryKey:     "C2y6yDjf5",
			KeyDatabase:       "database_id",
			KeyContainer:      "container_it",
			KeyPartitionValue: "partVal_test",
		}
		want = Config{
			URI:            "https://localhost:8081",
			PrimaryKey:     "C2y6yDjf5",
			Database:       "database_id",
			Container:      "container_it",
			PartitionValue: "partVal_test",
		}
	)

	var got Config
	err := sdk.Util.ParseConfig(ctx, raw, &got)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())

		return
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
}
