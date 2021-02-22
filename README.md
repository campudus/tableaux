# Tableaux [![Build Status](https://travis-ci.com/campudus/tableaux.svg?branch=master)](https://travis-ci.com/campudus/tableaux) [![Coverage Status](https://coveralls.io/repos/campudus/tableaux/badge.svg?branch=master&service=github)](https://coveralls.io/github/campudus/tableaux?branch=master) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/07e1410aa5404dd29eaa0b569d55a6de)](https://www.codacy.com/gh/campudus/tableaux/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=campudus/tableaux&amp;utm_campaign=Badge_Grade)

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Setup

At first you need to setup your database and create a new `conf.json` based on `conf-example.json`.
After that you can need to call `POST /system/reset` once to initialize system tables. If you wish you can fill in the demo data with `POST /system/resetDemo`.

## Update

If you upgrade from an older schema version you need to call `POST /system/update` before that. Schema will be upgraded automatically.

## Build & Test

Tableaux uses gradle to build a so called **fat jar** which contains all runtime dependencies. You can find it in `build/libs/tableaux-fat.jar`. The gradle task `build` needs a running PostgreSQL and the `conf-test.json` must be configured correct.

```
./gradlew clean build
```

Build without running tests:

```
./gradlew clean assemble
```

## Run as fat jar

To execute the **fat jar** call it like this:

```
$ java -jar tableaux-fat-0.1.0.jar -conf ../../conf.json
```

## Run in development

```
./gradlew run
```

or with automatic redeploy on code changes

```
./gradlew runRedeploy
```

## Run with different `conf` file

```
./gradlew run -Pconf='other.json'
```

## Highlevel Features

* Content Creation System
* Content Translation System
* Digital Asset Management
* Editing Publishing Workflow
* Workspaces & Custom Projections

## License

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
