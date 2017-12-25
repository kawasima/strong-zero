# StrongZero

StrongZero is a synchronization tool for microservices.

- Easy setup 
- Zero data loss

## Get started

Create tables in producer's schema and consumer's schema.

### Producer

PRODUCED_ZERO

|Name|Type|
|:---|:---|
|id|VARCHAR(32)|
|type|VARCHAR(32)|
|message|BLOB|

### Consumer

CONSUMED_ZERO

|Name|Type|
|:-----|:-----|
|producer_id|VARCHAR(16)|
|last_id|VARCHAR(32)|


## Examples

example.consumer.ExampleConsumer - Consumer example
example.producer.ExampleProducer - Producer example

