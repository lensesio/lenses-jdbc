[<img src="https://img.shields.io/badge/docs--orange.svg?"/>](http://lenses.stream/dev/jdbc/index.html)
[<img src="https://img.shields.io/maven-central/v/com.landoop/lenses-jdbc.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22lenses-jdbc%22)
[![Build Status](https://travis-ci.org/Landoop/lenses-jdbc.png?branch=master)](https://travis-ci.org/Landoop/lenses-jdbc)

# LSQL JDBC Library

![alt text][logo]

[logo]: https://github.com/Landoop/lenses-jdbc/blob/master/LSQL_JDBC.jpg "LENSES JDBC for Apache Kafka"


A JDBC 4.0 compliant driver for [Lenses](https://www.landoop.com/kafka-lenses/), suitable for any application that uses the JDBC interface, to communicate with Apache Kafka via the Lenses platform.

Users of this library can:

* Select from topics
* Insert into topics
* Use prepared statements
* Use batched inserts
* Fetch metadata around topics
* Fetch metadata around messages

## Documentation

Documentation can be found [here](http://lenses.stream/dev/jdbc/index.html).

## Documentation

Download can be found [in Maven Central](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.landoop%22%20AND%20a%3A%22lenses-jdbc%22).

## Requirements

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

# Testing
For testing it requires the Lenses Box to be running locally on http://localhost:3030

```bash
gradle clean test
```
