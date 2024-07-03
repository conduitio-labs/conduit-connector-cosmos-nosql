// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk/cmd/paramgen

package source

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"batchSize": {
			Default:     "1000",
			Description: "The size of an element batch.",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationGreaterThan{Value: 0},
				sdk.ValidationLessThan{Value: 100001},
			},
		},
		"container": {
			Default:     "",
			Description: "The name of a container the connector should work with.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"database": {
			Default:     "",
			Description: "The name of a database the connector should work with.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"keys": {
			Default:     "",
			Description: "Comma-separated list of key names to build the sdk.Record.Key.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"metaProperties": {
			Default:     "false",
			Description: "metaProperties whether the connector takes the next automatically generated meta-properties: \"_rid\", \"_ts\", \"_self\", \"_etag\", \"_attachments\".",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{},
		},
		"orderingKey": {
			Default:     "",
			Description: "The name of a key that is used for ordering items.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"partitionValue": {
			Default:     "",
			Description: "The logical partition key value.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"primaryKey": {
			Default:     "",
			Description: "The key for authentication with Azure Cosmos DB.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"snapshot": {
			Default:     "true",
			Description: "Determines whether the connector takes a snapshot of all items before starting CDC mode.",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{},
		},
		"uri": {
			Default:     "",
			Description: "The connection uri pointed to an Azure Cosmos DB for NoSQL instance.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
	}
}
