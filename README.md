# Tableaux ![Build Status](https://github.com/campudus/tableaux/actions/workflows/main_ci.yml/badge.svg?branch=master) [![Coverage Status](https://coveralls.io/repos/campudus/tableaux/badge.svg?branch=master&service=github)](https://coveralls.io/github/campudus/tableaux?branch=master) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/07e1410aa5404dd29eaa0b569d55a6de)](https://www.codacy.com/gh/campudus/tableaux/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=campudus/tableaux&amp;utm_campaign=Badge_Grade)

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Getting Started

### Preparing the environment

To get a working setup, you need a jdd. The easiest way to work with different java versions is to use [jEnv](https://github.com/jenv/jenv). It also works out of the box with gradle. If jEnv doesn't pick up the locally configured JDK automatically you can use the following command to set it up manually:

```sh
jenv enable-plugin gradle
```

To check if gradle is working, you can use the following command, which will print all the infos and versions:

```sh
gradlew -v

> ------------------------------------------------------------
> Gradle 7.4.1
> ------------------------------------------------------------
> 
> Build time:   2022-03-09 15:04:47 UTC
> Revision:     36dc52588e09b4b72f2010bc07599e0ee0434e2e
> 
> Kotlin:       1.5.31
> Groovy:       3.0.9
> Ant:          Apache Ant(TM) version 1.10.11 compiled on July 10 2021
> JVM:          17.0.2 (Homebrew 17.0.2+0)
> OS:           Mac OS X 12.2.1 x86_64
```

### Setup

At first you need to setup your database and create a new `conf.json` based on `conf-example.json`.
After that you can need to call `POST /system/reset` once to initialize system tables. If you wish you can fill in the demo data with `POST /system/resetDemo`.

### Update DB schema (optionally)

If you upgrade from an older schema version you need to call `POST /system/update` before that. Schema will be upgraded automatically.

## Build & Test

Tableaux uses gradle to build a so called **fat jar** which contains all runtime dependencies. You can find it in `build/libs/tableaux-fat.jar`. The gradle task `build` needs a running PostgreSQL and the `conf-test.json` must be configured correct. Requests in auth tests must contain an accessToken. For simplicity this accessToken is generated within a test helper with a hardcoded key pair. For the accessToken to match the pub key, the auth configuration for testing must always be the same as configured in `conf-test-example.json`.

```bash
./gradlew clean build
```

Build without running tests:

```bash
./gradlew clean assemble
```

## Tests (with custom config)

Tests use their own separate config, default configuration file is `conf-test.json`.
Specific config can be passed via arguments.

Run tests:

```bash
./gradlew test -Pconf='custom.json'
```

To run a single test use the following command:

```bash
# full package and test name or wildcard with *, e.g.:
./gradlew test --rerun-tasks --tests="*deleteTable_validRole*" --info"
```

## Run as fat jar

To execute the **fat jar** call it like this from project root:

```bash
java -jar ./build/libs/grud-backend-0.1.0-fat.jar -conf ../../conf.json
# with custom logging properties
java -jar -Djava.util.logging.config.file=./local_logging.properties ./build/libs/grud-backend-0.1.0-fat.jar -conf ./conf.json
```

## Run in development

```bash
./gradlew run
```

or with automatic redeploy on code changes

```bash
./gradlew runRedeploy
```

## Run with different `conf` file

```bash
./gradlew run -Pconf='other.json'
```

## Authentication and permission handling

TODO add documentation for authentication and permission handling. Currently the docs are filed in confluence and hackmd.io.

## Highlevel Features

* Content Creation System
* Content Translation System
* Digital Asset Management
* Editing Publishing Workflow
* Workspaces & Custom Projections

## License

```txt
Copyright 2016-present Campudus GmbH.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
