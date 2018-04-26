* [1. Preface](#1-preface)
  * [1.1. Information is widely spread across the company](#11-information-is-widely-spread-across-the-company)
  * [1.2. Connecting the dots](#12-connecting-the-dots)
  * [1.3. Editing / Publishing Separation](#13-editing--publishing-separation)
* [2. Getting started](#2-getting-started)
  * [2.1. Data retrieval](#21-data-retrieval)
     * [Overview](#overview)
     * [Retrieving tables](#retrieving-tables)
     * [Retrieving columns](#retrieving-columns)
     * [Retrieving rows](#retrieving-rows)
     * [Retrieving a specific column](#retrieving-a-specific-column)
     * [Retrieving a specific row](#retrieving-a-specific-row)
     * [Retrieving a specific cell](#retrieving-a-specific-cell)
  * [2.2. Data types &amp; column kinds](#22-data-types--column-kinds)
     * [Primitive data types](#primitive-data-types)
        * [Examples](#examples)
     * [Complex data types](#complex-data-types)
        * [<code>multi-language</code> and <code>multi-country</code>](#multi-language-and-multi-country)
        * [<code>link</code>](#link)
        * [<code>concat</code> and <code>group</code>](#concat-and-group)
        * [<code>attachment</code>](#attachment)

# 1. Preface

GRUD is an acronym and stands for "generic relational enterprise database". The following document should give a short introduction to GRUD and its principles.

## 1.1. Information is widely spread across the company
In today's companies data and information is widely spread across divisions and departments. We saw the need for storing enterprise data in a structured and centralized way. A company could and should take advantage of connecting all this widespread data.

The most valuable asset in your business is data. Managing data in a single source of truth is the crucial part of handling the complexity in today's rising demand for a digital Omnichannel strategy. GRUD enables you to spread content to multiple channels from one source. 

## 1.2. Connecting the dots
One of the main ideas is to store enterprise data in simple tables which are interconnected — which do have relations. You could start with simple and unconnected tables. Connecting that simple data in the right way enables you to easily leverage from rich product information. This idea is not new, but every relational database (Oracle, PostgreSQL, MySQL, etc.) out there shows how powerful it can be. We took this idea and its simple core concepts and built a user & consumer centered enterprise database. The core parts of GRUD consists of a RESTful API and an easy-to-use web-based user interface.

## 1.3. Editing / Publishing Separation
Another driving force behind GRUD's software architecture is the principle "Separation of Concerns". Traditional content management systems ship both "content editing" & "content publishing" in one piece of software. Both systems are interleaved but still serve highly different use cases. Content editing and content publishing in an omnichannel universe do have totally different requirements. The life-cycle of e.g. a website is different from a central enterprise database like GRUD.

# 2. Getting started

> All the following URLs are relative to your API endpoint.
> 
> E.g. https://example.org/api

## 2.1. Data retrieval

The schema is structured in tables and columns. A table consists of a set of columns and of course its rows. All API endpoints are generic and therefore are not bound to your schema at all.

### Overview

Here is a short overview about the generic API endpoints which are used to retrieve data from GRUD. 

* `/tables[/:tableid]`
  * Most basic structure is a table. These endpoints give you metadata about a table, like visibility and grouping.
* `/tables/:tableid/columns[/:columnid]`
  * Gives you information about how to process and understand data from a specific table.
* `/tables/:tableid/rows[/:rowid]`
  * Raw and structured content and metadata like flags or annotations.
  
To fully process a table and its rows you need to call these endpoints in order. That means you first have to get a tables unique ID to get all the columns. After that you can call the `/rows` endpoint of a specific table and process all the rows with the given information about the columns.

The next examples will show you how to traverse the data structure in more detail.

### Retrieving tables

`GET /tables`

Call this endpoint to retrieve all tables. Tables are the most basic structure in GRUD.  
In most instances there are many tables which store fundamental information like a list of countries or a list of vendors and some tables which combine these information to a rather comprehensive data structure.

```
{
  "tables": [
    {
      "id": 1, // unique table ID
      "name": "country", // internal and unique table name
      
      //
      // Meta information:
      //
      "hidden": false,
      "displayName": { // user-friendly table name
        "de": "Land",
        "en": "Country"
      },
      "description": { // user-friendly table description
        "de": "Liste aller benötigten Länder"
        // ... more languages optional
      },
      "group": { // tables can be grouped
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

### Retrieving columns

`GET /tables/:tableid/columns`

Call this endpoint of a specific table to retrieve its columns. The different data type will be exampled later. Each table consists of at least one column. Additionally at least one of the tables columns does have the `identifier` flag. The `identifier` flag is used to distinguish one row to another. Multiple `identifier` columns are possible. The `identifier` value of a row is be used as a link value. This will be explained in the [data types chapter](#link) with examples. 

```
{
  "columns": [
    {
      "id": 1, // unique column ID
      "ordering": 1, // ordering can be ignored; used to determine sorting of columns
      "name": "name", // internal and unique column name
      "kind": "shorttext", // data type
      "multilanguage": true, // deprecated, false if languageType equals neutral
      "languageType": "language", // language type can be neutral, language, or country
      "identifier": true, // flag if this column is used to identify a row
      "frontendReadOnly": true, // flag whether this column is marked as read only for the frontend (default: 'false')
      "displayName": { // user-friendly column name
        "de": "Name",
        "en": "Name",
        "fr": "Nom",
        "es": "Nombre"
      },
      "description": { // user-friendly column description
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

### Retrieving rows

`GET /tables/:tableid/rows`

Call this endpoint to retrieve all rows of a specific table. Most important parts of a row object are its `id` and `values`. The `id` is the unique row ID which is needed to identify the row. The `values` array contains one value object for each column. The order of `values` is exactly the same as of `columns`. In this case the table has two columns and therefore each `values` array has the length two. The different [data types](#22-data-types--column-kinds) will be described later on.

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

### Retrieving a specific column

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

### Retrieving a specific row

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

### Retrieving a specific cell

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

Each column has a specific data type defined in its `kind` field. There are a number of different data types which will be described in this chapter.

### Primitive data types

Currently there are a few primitive data types. All primitive data types can be used in a multi-language column or a language neutral column.

* `text`, `shorttext`, and `richtext`
* `numeric` and `currency`
* `date` and `datetime`
* `boolean`

#### Examples 

For text there are three different column kinds. `text`, `shorttext`, and `richtext`. All three are syntactically equal but semantically different. Frontend interfaces can use this information to display the text differently.

* `shorttext` should only contain a word or a short sentence — but no line breaks
* `text` is meant for texts without formatting
* `richtext` is meant for text with markdown syntax

```
// shorttext example
"This is a text"
```

The data types `numeric` and `currency` are meant for storing numerical values like integers and floats. `currency` can be used to store prices. It is often combined with `"languageType": "country"` as most prices are country specific.

```
// numeric example
1337.42
```

To store date and time information the data types `date` and `datetime` can be used. For representation of dates and times we use ISO 8601. Combined date and time always includes UTC time zone information.

```
// date example
"2017-10-01"
```

```
// datetime example
"2017-10-01T13:37:42.000Z"
```

Another primitive data type is `boolean` for simple flags. For example if an entity of your data "uses X" or "has Y".

```
// boolean example
true
```

### Complex data types

#### `multi-language` and `multi-country`

Primitive data types can be used in combination with the `"languageType": "language"` or `"languageType": "country"` in each column. Here is an example of a multi-language column definition:

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

The keys of such multi-language value object are called language tags ([RFC 5646 Tags for Identifying Languages](https://tools.ietf.org/html/rfc5646)). Here are some examples:

* `de` represents German.
* `de-DE` represents German (`de`) as used in Germany (`DE`).
* `de-AT` represents German (`de`) as used in Austria (`AT`).
* `en` represents English.
* `en-US` represents English (`en`) as used in United States (`US`).

In the majority of cases multi-language data is provided for a specific language and not for a country specific language. In most GRUD instances we would not recommend country specific values for `de-DE` or `de-AT` for example. Having country specific languages makes the data very flexible but can as well lead to an increase in the maintenance overhead for people inserting data to GRUD. It would only make sense if the difference in languages is substantial.

In addition to multi-language, `languageType` can be set to `country`. If a column has this `languageType` there needs to be another field called `countryCodes`. Here is an example of a multi-country column definition:

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

Each value for a multi-country currency column is a JSON object like this:

```
{
  "DE": 47.11,
  "AT": 47.11,
  "US": 40.99
}
```

#### `link`

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

A link column always points to a specific table — in this case table `1`. A link value is the association between at least two rows. In the following example there is one association, one link to the foreign row with the ID `13`. The value used here is the `identifier` value of the foreign row as example in the [data retrieval chapter](#21-data-retrieval). To retrieve the full row you can call `GET /tables/1/rows/13`.

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

#### `concat` and `group`

This column kinds combine multiple columns into one column. Setting multiple `identifier` columns in a table will automatically add a `concat` column at the beginning of the columns array. A `concat` column combines the values of the `identifier` columns into one column and is used to reference a foreign row in a link. The `group` column lets you combine multiple columns into one, for example grouping three columns `height`, `length`, and `depth` together into a single field for the UI as `<height> x <length> x <depth>`.

Here is an example of a `concat` cell which combines three columns (`link`, `shorttext`, and `numeric`):

```
[
  [
    {
      "id": 10,
      "value": "Bosch"
    }
  ],
  "Active Cruise",
  250
]
```

#### `attachment`

An attachment column is used to link files from the media management to a specific cell. For more information about the media management API look up the Swagger API documentation. It can be found at `/docs` relative to your GRUD API endpoint.

Here is an example of an attachment cell:

```
{
  "uuid": "5a7ae8a7-d2b8-441c-8223-685762297907", // unqiue file uuid
  "url": { // public file urls for different languages
    "de": "/files/5a7ae8a7-d2b8-441c-8223-685762297907/de/Test+PDF.pdf"
  },
  "folder": 114, // unique folder id which contains this document
  "folders": [55, 77, 114], // path of folder hierachry
  "title": {
    "de": "Test PDF"
  },
  "description": {
  },
  "internalName": { // internal file name, can be ignored
    "de": "f6a4f809-5a5c-46a8-a764-2fb291ec9a8a.pdf"
  },
  "externalName": { // external file name, used for public urls
    "de": "Test PDF.pdf"
  },
  "mimeType": {
    "de": "application/pdf"
  },
  "createdAt": "2017-03-23T10:01:09.998+01:00",
  "updatedAt": "2017-03-23T10:01:47.604+01:00"
}
```
