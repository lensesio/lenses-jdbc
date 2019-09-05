# 1. Use only the Value projection

Date: 2019-08-27

## Status

Accepted

## Context


Kafka record contains amongst other things a Key and a Value. This does not go well with a `SELECT *` 
when using tabular data structure like JDBC requires.

The previous context relies on building a JDBC row by merging the data from Key and Value.
This results in problems on retrieving the column based on the name and index since it relies only on the schema for the Value.
Merging the Key and Value schema is not trivial and neither is intuitive for the user. 

The engine returns a lot more information including the metadata for a record. 

## Decision

Only consider the data returned in the Value. 

## Consequences

To get the Key value/fields it requires the user to perform a projection lifting the key:

```$sql
SELECT *, _key as key
FROM curious_eyes
```

The same applies for getting the record metadata

```$sql
SELECT *, _meta.offset as _offset, _meta.partition as _partition
FROM curious_eyes
```