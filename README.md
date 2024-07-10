# Conduit Connector for Azure Cosmos DB for NoSQL

## General

Azure Cosmos DB for NoSQL connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both,
a Source and a Destination Azure Cosmos DB for NoSQL connectors.

## Prerequisites

- [Go](https://go.dev/) 1.21
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.55.2

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all unit, integration and acceptance tests. To run integration and acceptance tests, set the URI
and PRIMARY KEY of the database to the environment variables as a `COSMOS_NOSQL_URI` and a `COSMOS_NOSQL_PRIMARY_KEY`,
respectively.

## Source

The Cosmos DB for NoSQL Source Connector connects to Azure Cosmos DB for NoSQL container with the
provided `uri`, `primaryKey`, `database` and `container`. Upon starting, the Source takes a snapshot of a given
container in the database, then switches into CDC mode. In CDC mode, the connector will only detect creating new items.

### Snapshot Capture

At the first launch of the connector, the snapshot mode is enabled and the last value of the `orderingKey` is stored
to the position, to know the boundary of this mode. The connector reads all items of a container in batches, using a
keyset pagination, limiting the items by `batchSize` and ordering by `orderingKey`. The connector stores the
last processed element value of an `orderingKey` in a position, so the snapshot process can be paused and resumed
without losing data. Once all items in that initial snapshot are read the connector switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding `"snapshot": false` to the Source configuration.

### Change Data Capture

In this mode, only items added after the first launch of the connector are moved in batches. It’s using a keyset
pagination, limiting by `batchSize` and ordering by `orderingKey`.

### Configuration

| name             | description                                                                                                                                                          | required | example                          |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------------------------------|
| `uri`            | Connection uri pointed to an Azure Cosmos DB for NoSQL instance.                                                                                                     | **true** | `https://bfb.doc.azure.com:443/` |
| `primaryKey`     | Key for authentication with Azure Cosmos DB.                                                                                                                         | **true** | `OJI6ALpypaOWACDbBh0kuA==`       |
| `database`       | Name of a database the connector should work with.                                                                                                                   | **true** | `db_name`                        |
| `container`      | Name of a container the connector should work with.                                                                                                                  | **true** | `collection_name`                |
| `partitionValue` | Logical partition key value.                                                                                                                                         | **true** | `partValue`                      |
| `orderingKey`    | Name of a key that is used for ordering items.                                                                                                                       | **true** | `id`                             |
| `keys`           | Comma-separated list of key names to build the `sdk.Record.Key`.                                                                                                     | false    | `id,name`                        |
| `snapshot`       | Determines whether the connector takes a snapshot of all items before starting CDC mode. By default is `true`.                                                       | false    | `false`                          |
| `metaProperties` | MetaProperties whether the connector takes the next automatically generated meta-properties: "_rid", "_ts", "_self", "_etag", "_attachments". By default is `false`. | false    | `true`                           |
| `batchSize`      | Size of items batch. Min is 1 and max is 100000. By default is `"1000"`.                                                                                             | false    | `100`                            |

### Key handling

The connector builds `sdk.Record.Key` as `sdk.StructuredData`. The keys of this field consist of elements of the `keys`
configuration field. The values of `sdk.Record.Key` field are taken from `sdk.Payload.After` by the keys of this field.

### Container Name

For each record, the connector adds a `cosmos-nosql.container` property to the metadata that contains the container
name.

## Destination

The Cosmos DB for NoSQL Destination Connector takes an `sdk.Record` and parses it into a valid Cosmos DB for NoSQL
query. The `sdk.Record.Payload.After` must have a partition key value pair for `create` and `update` operations.

### Configuration Options

| name             | description                                                      | required | example                          |
|------------------|------------------------------------------------------------------|----------|----------------------------------|
| `uri`            | Connection uri pointed to an Azure Cosmos DB for NoSQL instance. | **true** | `https://bfb.doc.azure.com:443/` |
| `primaryKey`     | Key for authentication with Azure Cosmos DB.                     | **true** | `OJI6ALpypaOWACDbBh0kuA==`       |
| `database`       | Name of a database the connector should work with.               | **true** | `db_name`                        |
| `container`      | Name of a container the connector should work with.              | **true** | `collection_name`                |
| `partitionValue` | Logical partition key value.                                     | **true** | `partValue`                      |

### Key handling

The mandatory parameter of the conditions of all operations is `id`. The connector first tries to take the `id`
parameter value from the `sdk.Record.Key` and then, if nothing was found, from the `sdk.Record.Payload.After`. If they
both do not contain the `id` key, the connector returns an error.

### Container Name

If the record contains a `cosmos-nosql.container` property in its metadata, it will work with this container, otherwise,
it will fall back to use the `container` configured in the connector.
