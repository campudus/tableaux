# Tableaux [![Build Status](https://travis-ci.org/campudus/tableaux.svg)](https://travis-ci.org/campudus/tableaux) [![Coverage Status](https://coveralls.io/repos/campudus/tableaux/badge.svg?branch=master&service=github)](https://coveralls.io/github/campudus/tableaux?branch=master)

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Setup

At first you need to setup your database and create a new `conf.json` based on `conf-example.json`.
After that you can need to call `POST /reset` once to initialize system tables. If you wish you can fill in the demo data with `POST /resetDemo`.

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
$ java -jar tableaux-fat.jar -conf ../../conf.json
```

## Run in development

```
./gradlew run
```

or with automatic redeploy on code changes

```
./gradlew runRedeploy
```

## Highlevel Features

* Content Creation System
* Content Translation System
* Digital Asset Management
* Editing Publishing Workflow
* Workspaces & Custom Projections
