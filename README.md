# postgres-cdc

[![Actions Status](https://github.com/rieske/postgres-cdc/workflows/master/badge.svg)](https://github.com/rieske/postgres-cdc/actions)

Java library that utilizes [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html) 
feature to implement [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture).

Once logical replication is configured on the PostgreSQL server, this library can subscribe to changes
in the specified tables. 
The change events are streamed in real time and can be relayed to message brokers
as they occur, allowing to implement the [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) 
pattern.

## Prerequisites

### PostgreSQL

PostgreSQL version 13.12 or later.
Note 13.12 is the earliest one that this library is tested against at the time of writing.
In theory, it may work with PostgreSQL 9.5 and above.

### Logical replication enabled

Logical replication must be [configured](https://www.postgresql.org/docs/current/logical-replication-config.html#LOGICAL-REPLICATION-CONFIG-PUBLISHER) 
on the PostgreSQL server.

If you are using AWS Aurora, see [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure)
for instructions to enable logical replication.

### [wal2json](https://github.com/eulerto/wal2json) plugin installed

This library relies on the [wal2json](https://github.com/eulerto/wal2json) PostgreSQL output plugin to convert the 
WAL data to JSON.

The `wal2json` plugin should be available in AWS Aurora databases by default.

To install the plugin on a self-hosted instance, refer to the plugin's documentation.

## Usage


## Testing

In order to write integration tests for code that uses the `postgres-cdc` library, the test database
must have logical replication enabled and have the `wal2json` plugin installed.

Refer to the [test Dockerfiles](lib/src/test/resources/postgres/) in this repository that produce configured PostgreSQL images.

