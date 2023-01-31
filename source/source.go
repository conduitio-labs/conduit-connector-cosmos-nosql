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

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Iterator is an interface needed for the [Source].
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next() (sdk.Record, error)
}

// Source is an Azure Cosmos DB for NoSQL source connector.
type Source struct {
	sdk.UnimplementedSource

	iterator Iterator
	config   Config
}

// New creates a new instance of the [Source].
func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Source].
func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

// Configure parses and initializes the [Source] config.
func (s *Source) Configure(ctx context.Context, raw map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring an Azure Cosmos DB for NoSQL Source...")

	if err := sdk.Util.ParseConfig(raw, &s.config); err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	return nil
}

// Open parses the position and initializes the iterator.
func (s *Source) Open(ctx context.Context, sdkPosition sdk.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an Azure Cosmos DB for NoSQL Source...")

	var err error

	s.iterator, err = newIterator(ctx, s.config, sdkPosition)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	return nil
}

// Read returns the next [sdk.Record].
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from an Azure Cosmos DB for NoSQL Source...")

	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return record, nil
}

// Ack just logs the debug event with the position.
func (s *Source) Ack(ctx context.Context, sdkPosition sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(sdkPosition)).Msg("got ack")

	return nil
}

// Teardown just logs the info event.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down an Azure Cosmos DB for NoSQL Source")

	return nil
}
