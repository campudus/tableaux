# tableaux [![Build Status](https://travis-ci.org/campudus/tableaux.svg)](https://travis-ci.org/campudus/tableaux)

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Setup

At first you need to setup your database and create a new `conf.json` based on `conf-example.json`. After that you can need to call `POST /reset` once to create the systems. If you wish you can fill in the demo data with `POST /resetDemo`.

## FatJar

Run `sbt` and create make a new `fatJar`. You can find it in `target/deploy/`.

```
sbt clean fatJar
```

To execute the `fatJar` you can call it like this:

```
$ java -jar tableaux_2.11-0.1.0-fat.jar -conf ../../conf.json`
```
