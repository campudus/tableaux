# 1. Introduction

GRUD is an acronym and stands for "generic relational enterprise database". Following document should give a short introduction to GRUD and its principles.

## 1.1. Information is widely spread across the company
In today's companies data and information is widely spread across divisions and departments. We saw the need for storing Excel-like data in a structured and centralized way. A company could and should take advantage of connecting all this widespread data.

## 1.2. Connecting the dots
One of the main ideas is to store enterprise data in simple tables which are interconnected — which do have relations. You could start with simple and dumb tables. If you start to connect the data in the right way, you will leverage from rich product information. This idea is not new, but every relational database (Oracle, PostgreSQL, MySQL, etc.) out there shows how powerful it can be. We took this idea and its simple core concepts and built a user & consumer centered enterprise database. The core parts of GRUD consists of an RESTful API and an easy-to-use web-based user interface.

## 1.3. Editing / Publishing Separation
Another driving force behind GRUD's software architecture is the principle "Separation of Concerns". Traditional content management systems ship both "content editing" & "content publishing" in one piece of software. Both systems are interleaved but still serve highly different use cases. Content editing and content publishing in an omnichannel universe do have totally different requirements. The life-cycle of e.g. a website is different to a central enterprise database like GRUD.

# 2. Getting started

> All the following URLs are relative to your API endpoint.
> 
> E.g. https://example.org/api

## 2.1. Data retrieval

First of all the schema is structured in tables and columns. A table consists of a set of columns and of course its rows. All API endpoints are generic and therefore are not bound to your schema at all. The next three examples will show you how to traverse the data structure.

`GET /tables`

Call this endpoint to retrieve all tables.

```
{
  "tables": [
    {
      "id": 1, // Unique table ID
      "name": "country", // Internal and unique table name
      
      //
      // Meta information:
      //
      "hidden": false, // 
      "displayName": { // User-friendly table name
        "de": "Land",
        "en": "Country"
      },
      "description": { // User-friendly table description
        "de": "Liste aller benötigten Länder"
        // ... more languages optional
      },
      "group": { // Tables can be grouped
        "id": 10,
        "displayName": {
          "de": "Allgemein",
          "en": "Common"
        },
        "description": {}
      }
    }
    // ... more tables to come
  ]
} 
```

`GET /tables/:tableid/columns`

Call this endpoint of a specific tables to retrieve its columns. The different data type will be exampled later. Each table consists of at least one column. Additionally at least one of the tables columns does have the `identifier` flag. The `identifier` flag is used to distinguish one row to another. Multiple `identifier` columns are possible. The `identifier` value of a row is be used as a link value — this will be example in the data types chapter. 

```
{
  "columns": [
    {
      "id": 1, // Unique column ID
      "ordering": 1, // Ordering can be ignored; used to determine sorting of columns
      "name": "name", // Internal and unique column name
      "kind": "shorttext", // Data type
      "multilanguage": true, // Deprecated, false if languageType equals neutral
      "languageType": "language", // Language type can be neutral, language, or country
      "identifier": true, // Flag if this column is used to identify a row
      "displayName": { // User-friendly table name
        "de": "Name",
        "en": "Name",
        "fr": "Nom",
        "es": "Nombre"
      },
      "description": { // User-friendly table description
        // ...
      }
    },
    {
      "id": 2,
      "name": "anotherColumn",
      "kind": "shorttext",
      "multilanguage": false,
      "languageType": "neutral"
      // ...
    }
    // ... more columns to come
  ]
}
```

`GET /tables/:tableid/rows`

Call this endpoint to retrieve all rows of a specific table. Most important part of a row object is `id` and `values`. The `id` is the unique row ID which is needed to identify the row. The `values` array contains one value object for each column. The order of `values` is exactly the same as of `columns`. In this case the table has two columns and therefore each `values` array has the length two. The different data types will be described later on.

```
{
  "page": { // result can be paged with two query parameters offset and limit
    "offset": null,
    "limit": null,
    "totalSize": 18 // total size of this table
  },
  "rows": [
    { // row object
      "id": 24, // Unique column ID
      "values": [
        {
          "de": "Schweden",
          "es": "Suecia",
          "fr": "Suède",
          "it": "Svezia"
        },
        "2nd value"
      ]
    },
    {
      "id": 25,
      "values": [
        {
          "de": "Deutschland",
          "en": "Germany"
        },
        "2nd value"
      ]
    }
    // ... more rows to come
  ]
}
```

`GET /tables/:tableid/columns/:columnid`

Call this endpoint to retrieve the definition of one specific column. Same column object definition as above. 

```
{
  "id": 1,
  "ordering": 1,
  "name": "name",
  "kind": "shorttext"
  // ...
}
```

`GET /tables/rows/:rowid`

Call this endpoint to retrieve a specific row. Same row object definition as above.

```
{
  "id": 24,
  "values": [
    {
      "de": "Schweden",
      "es": "Suecia",
      "fr": "Suède",
      "it": "Svezia"
    },
    "2nd value"
  ]
}
```

`GET /tables/:tableid/columns/:columnid/rows/:rowid`

Call this endpoint to retrieve a single value of a specific cell. Value object can be different for the various data types.

```
{
  "value": {
    "de": "Schweden",
    "es": "Suecia",
    "fr": "Suède",
    "it": "Svezia"
  }
}
```

## 2.2. Data types & column kinds

Each column has a specific data types defined in its `kind` field. There are a number of different data types which will be described in this chapter.

### Primitive data types

Currently there are a few primitive data types which can be described very easily. All primitive data types can be used in a multi-language column or a language neutral column.

* `text`, `shorttext`, and `richtext`
* `numeric` and `currency`
* `date`, and `datetime`
* `boolean`

#### Examples 

For text there are three different column kinds. `text`, `shorttext`, and `richtext`. All three are syntactically the same but semantically different.

* `shorttext` should only contain a word or a short sentence
* `text` is meant for texts without formation
* `richtext` is meant for text with markdown syntax

```
// shorttext example
"This is a text"
```

The data types `numeric` and `currency` are meant for storing numerical values like integers and floats. The data type `currency` is obviously for storing prices. It's often combined with `languageType` `country` because most prices are country specific.

```
// numeric example
1337.42
```

To store date and time information the data types `date`, and `datetime` can be used. 

```
// date example
"2017-10-01"
```

```
// datetime example
"2017-10-01T13:37:42.000Z"
```

Another primitive data type is `boolean`.

```
// boolean example
true
```

### Higher data types

#### `multi-language` and `multi-country`

As described above primitive data types can be used in combination with the `languageType` `language` or `country`. The `languageType` is defined for each column. Here is an example of a multi-language column definition:

```
{
  "id": 2,
  "name": "multilanguageColumn",
  "kind": "shorttext",
  "languageType": "language"
  // ...
}
```

Each value for such a multi-language text column is a JSON object like this:

```
{
  "de": "Deutsch",
  "en": "English",
  "es": "Español",
  "fr": "Français"
}
```

The keys of such a multi-language value object are called language tags. Here are some examples:

* `de` is German
* `de-DE` is German in Germany
* `de-AT` is German in Austria
* `en` is English
* `en-US` is American English

Most of the time multi-language data is provided for a specific language and not for a country specific language. Except for `en-US`.

Additionally to multi-language there is the `languageType` `country`. If a column has this language type there is also another field called `countryCodes`. Here is an example of a multi-country column definition:

```
{
  "id": 2,
  "name": "price",
  "kind": "currency",
  "languageType": "country",
  "countryCodes": [
    "DE",
    "AT",
    "US"
  ]
  // ...
}
```

Each value for such a multi-country currency column is a JSON object like this:

```
{
  "DE": 47.11,
  "AT": 47.11,
  "US": 40.99
}
```

### `link`

This is probably the most important column kind. It is used to make a relation between two tables. A link is a uni- or bidirectional association. Here is an example of a link column definition:

```
{
  "id": 8,
  "name": "country",
  "identifier": false,
  "kind": "link",
  "toTable": 1, // foreign table ID
  "toColumn": { // foreign column definition
    "id": 1,
    "name": "name",
    "identifier": true,
    "kind": "shorttext",
    "languageType": "language",
    "multilanguage": true
    // ...
  }
  // ...
}
```

A link column always points to a specific table — in this case the table `1`. A link value is the association between at least two rows. In the following example there is one association, one link to the foreign row with the ID `13`. The value used here is the `identifier` value of the foreign row as example in the data retrieval chapter. To retrieve the full row you can call `GET /tables/1/rows/13`.

```
[
  {
    "id": 13,
    "value": {
      "cs": "Česká republika",
      "de": "Tschechien",
      "en": "Czech Republic",
      "en-US": "Czech Republic",
      "es": "Chequia",
      "fr": "République Tchèque",
      "it": "Repubblica Ceca",
      "pl": "Czechy"
    }
  }
]
```

### `concat`, and `group`

This column kinds combines multiple columns into one column. The column kind `concat` is only used to combine multiple `identifier` columns into one column. Additionally there is the column kind `group` which can be used to combine multiple columns into one column for convenience.

### `attachment`

This column kind is used for linking files with their `uuid` to a specific cell. For more information please look up the Swagger API documentation.
