# tableaux

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Setup

At first you need to setup your database and create a new `conf.json` based on `conf-example.json`. After that you can need to call `POST /reset` once to create the systems. If you wish you can fill in the demo data with `POST /resetDemo`.