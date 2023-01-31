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
	"reflect"
	"testing"
)

func TestSource_Configure(t *testing.T) {
	t.Parallel()

	s := Source{}

	var (
		raw = map[string]string{
			ConfigKeyOrderingKey: "id",
			ConfigKeySnapshot:    "false",
			ConfigKeyBatchSize:   "5000",
		}
		want = Config{
			OrderingKey: "id",
			Snapshot:    false,
			BatchSize:   5000,
		}
	)

	err := s.Configure(context.Background(), raw)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())

		return
	}

	if !reflect.DeepEqual(s.config, want) {
		t.Errorf("got: %v, want: %v", s.config, want)
	}
}
