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
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// position is an iterator position.
type position struct {
	// LastProcessedValue is a value of the orderingKey key
	// of the last item that was processed.
	LastProcessedValue any `json:"lastProcessedValue"`
	// LatestSnapshotValue is a value of the orderingKey key
	// of the most recent value item at the moment the iterator is initialised.
	LatestSnapshotValue any `json:"latestSnapshotValue"`
}

// parsePosition converts an [sdk.Position] into a [position].
func parsePosition(sdkPosition sdk.Position) (*position, error) {
	pos := new(position)

	if sdkPosition == nil {
		return pos, nil
	}

	if err := json.Unmarshal(sdkPosition, pos); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into position: %w", err)
	}

	return pos, nil
}

// marshal marshals the underlying [position] into a [sdk.Position] as JSON bytes.
func (p position) marshal() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return positionBytes, nil
}
