[<img src="https://img.shields.io/badge/docs--orange.svg?"/>](http://lenses.stream/dev/jdbc/index.html)
[<img src="https://img.shields.io/maven-central/v/com.landoop/lenses-jdbc.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22lenses-jdbc%22)
[![Build Status](https://travis-ci.org/Landoop/lenses-jdbc.png?branch=master)](https://travis-ci.org/Landoop/lenses-jdbc)

# LSQL JDBC Library

![alt text][logo]

[logo]: https://github.com/Landoop/lenses-jdbc/blob/master/LSQL_JDBC.jpg "LENSES JDBC for Apache Kafka"


A JDBC 4.0 compliant driver for [Lenses](https://www.lenses.io/), suitable for any application that uses the JDBC interface, to communicate with Apache Kafka via the Lenses platform.

Users of this library can:

* Select from topics
* Insert into topics
* Use prepared statements
* Use batched inserts
* Fetch metadata around topics
* Fetch metadata around messages

## Documentation

Documentation can be found [here](https://docs.lenses.io/3.2/dev/jdbc/index.html).

## Download

Download from [Maven Central](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22io.lenses%22%20AND%20a%3A%22lenses-jdbc%22).

## Requirements

1. Java 1.8+
2. Gradle 4.9+

# Building

Requires Gradle 4.9+

To build:

```bash
./gradlew compileKotlin
```

For a fatJar:

```bash
./gradlew shadowJar
```

# Testing
For testing it requires the Lenses Box to be running locally on http://localhost:3030

```bash
./gradlew clean test
```

# License

The project is licensed under the Apache 2 license.
