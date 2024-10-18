// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package source

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigBatchSize      = "batchSize"
	ConfigContainer      = "container"
	ConfigDatabase       = "database"
	ConfigKeys           = "keys"
	ConfigMetaProperties = "metaProperties"
	ConfigOrderingKey    = "orderingKey"
	ConfigPartitionValue = "partitionValue"
	ConfigPrimaryKey     = "primaryKey"
	ConfigSnapshot       = "snapshot"
	ConfigUri            = "uri"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigBatchSize: {
			Default:     "1000",
			Description: "The size of an element batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 0},
				config.ValidationLessThan{V: 100001},
			},
		},
		ConfigContainer: {
			Default:     "",
			Description: "The name of a container the connector should work with.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigDatabase: {
			Default:     "",
			Description: "The name of a database the connector should work with.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigKeys: {
			Default:     "",
			Description: "Comma-separated list of key names to build the opencdc.Record.Key.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigMetaProperties: {
			Default:     "false",
			Description: "MetaProperties whether the connector takes\nthe next automatically generated meta-properties:\n\"_rid\", \"_ts\", \"_self\", \"_etag\", \"_attachments\".",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigOrderingKey: {
			Default:     "",
			Description: "The name of a key that is used for ordering items.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPartitionValue: {
			Default:     "",
			Description: "The logical partition key value.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPrimaryKey: {
			Default:     "",
			Description: "The key for authentication with Azure Cosmos DB.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigSnapshot: {
			Default:     "true",
			Description: "Determines whether the connector takes a snapshot\nof all items before starting CDC mode.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigUri: {
			Default:     "",
			Description: "The connection uri pointed to an Azure Cosmos DB for NoSQL instance.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
