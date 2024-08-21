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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer is a writer interface needed for the [Destination].
type Writer interface {
	Write(ctx context.Context, record sdk.Record) error
}

// Destination is an Azure Cosmos DB for NoSQL destination connector.
type Destination struct {
	sdk.UnimplementedDestination

	config Config
	writer Writer
}

// New creates a new instance of the [Destination].
func New() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Destination].
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

// Configure parses and initializes the [Destination] config.
func (d *Destination) Configure(_ context.Context, raw map[string]string) error {
	if err := sdk.Util.ParseConfig(raw, &d.config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening an Azure Cosmos DB for NoSQL Destination...")

	var err error

	d.writer, err = newWriter(d.config)
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes a record into a [Destination].
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	sdk.Logger(ctx).Debug().Msg("Write records to an Azure Cosmos DB for NoSQL...")

	for i, record := range records {
		if err := d.writer.Write(ctx, record); err != nil {
			return i, fmt.Errorf("write record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown just logs the info event.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down an Azure Cosmos DB for NoSQL Destination")

	return nil
}
