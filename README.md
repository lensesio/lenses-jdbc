[<img src="https://img.shields.io/badge/docs--orange.svg?"/>](http://lenses.stream/dev/jdbc/index.html)
[<img src="https://img.shields.io/badge/latest%20release-1.0-blue.svg?label=latest%20release"/>](https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22lenses-jdbc%22)


# LSQL JDBC Library

A JDBC 4.0 compliant driver for [Lenses](https://www.landoop.com/kafka-lenses/), suitable for any application that uses the JDBC interface, to communicate with Apache Kafka via the Lenses platform.

Users of this library can:

Select from topics
Insert into topics
Use prepared statements
Use batched inserts
Fetch metadata around topics
Fetch metadata around messages

## Documentation

Documenation can be found [here.](https:/lenses.stream/dev/jdbc/).

## Fetures

Users of this library can:

Select from topics
Insert into topics
Use prepared statements
Use batched inserts
Fetch metadata around topics
Fetch metadata around messages)

## Requirments

1. java 1.8+
2. gradle 3.5+

# Building

Requires grade 3.5+

To build:

```bash
gradle compileKotlin
```

For a fatJar:

```bash
gradle shadowJar
```

