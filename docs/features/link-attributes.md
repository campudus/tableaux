# Link attributes

A link — the connection between one row and one row in another table — can carry values of its own: a percentage, a date, or a free-text note per connection. Those values belong to neither of the two rows; they belong to the link between them.

The *definitions* hang off the link relation and are therefore identical for the link column and its backlink column. The *values* hang off each individual link and are readable and editable from both sides.

> **What this document owns.** Flows and the rules that hold *between* fields: the positional contract, what `null` means, how a definition change migrates stored values, which verb updates what. The field-by-field reference — every property, type, and example — lives in `swagger.json`, served at `/docs/swagger.json`. When the two disagree, swagger is right about field shapes and this document is right about behaviour. Please keep it that way: do not grow a field table here.

## How the pieces fit together

Take a table `product` with a link column `suppliedBy` pointing at a table `supplier`. Creating that column also creates the backlink column `supplies` in `supplier`. The two columns are not two relationships — they are two views of one **link relation**.

**Definitions are stored once, on that link relation.** Define a link attribute `percentage` on `suppliedBy`, and it appears as `linkAttributes` on `suppliedBy` *and* on `supplies`, identically. Change it from either side and it changes for both. There is no way for the two directions to disagree, because there is only one stored copy.

**Values are stored one level down, on each individual link.** If product 1 is linked to supplier 4 and to supplier 9, and product 2 is also linked to supplier 4, those are three separate links, and each carries its own `attributes` array:

| Link | `attributes` in the cell response | Meaning |
| --- | --- | --- |
| product 1 → supplier 4 | `[50]` | the attribute has a value |
| product 1 → supplier 9 | `[null]` | the value was explicitly cleared |
| product 2 → supplier 4 | key absent | nothing was ever stored for this link |

**The array is bound to the definition list by position, not by name.** `attributes[0]` is the `percentage` value precisely because `percentage` is `linkAttributes[0]`. Nothing in the value array says which attribute it belongs to; the definition list at the same index is the only thing that gives it meaning. A second definition would take slot 1 on every link, and so on — though today a link column may define only one, see [Current rollout limits](#current-rollout-limits). Everything else in this document follows from the positional binding — see [ADR 0007](../adr/0007-link-attribute-values-are-positional.md).

---

## Part 1 — Using link attributes

### What a link attribute is, and what it is not

A link attribute is deliberately **not a column**. It has no `id`, no `ordering`, and no row in `system_columns`; it is referenced only by its `name`. That has consequences you will notice: you cannot sort by it, it does not appear in the columns list, and renaming it is a normal edit rather than a delete-and-recreate. The reasoning is recorded in [ADR 0006](../adr/0006-link-attributes-are-not-columns.md).

### Three different things are called `attributes`

The key name is overloaded. When you are staring at a response, this is which is which:

| Where you see it | What it is |
| --- | --- |
| `column.attributes` | A free-form JSON object storable on **any** column. Predates link attributes and has nothing to do with them. |
| `column.linkAttributes` | The **definitions**: what this link column's links may carry. |
| `attributes` inside a link value, and the body of the dedicated endpoint | The **values** on one individual link. |

### Defining link attributes

Pass `linkAttributes` when creating a link column:

```
POST /tables/1/columns
```

```json
{
  "columns": [
    {
      "name": "Test Link 1",
      "kind": "link",
      "toTable": 2,
      "singleDirection": false,
      "linkAttributes": [
        {
          "name": "percentage",
          "displayName": {"de-DE": "Prozentanteil"},
          "kind": "integer",
          "multilanguage": false
        }
      ]
    }
  ]
}
```

or change them afterwards with a `POST` on the column. There, the field is tri-state:

| `linkAttributes` in the request | Effect |
| --- | --- |
| omitted | definitions untouched |
| an array | replaces the definitions and migrates stored values (see below) |
| `[]` or `null` | deletes the definitions **and wipes every stored value** |

Allowed `kind`s are `text`, `numeric`, `integer`, `boolean`, `date` and `datetime` — notably *not* `shorttext`, `richtext` or `currency`. A `name` must match `\w+` (letters, digits, underscores) and be unique within the column. The `\w+` restriction exists because the name is the handle used in format patterns, where a dot would make `{{attributes.a.b}}` ambiguous.

### Format patterns on link columns

`formatPattern` now works for link columns too, with two placeholders: `{{value}}` for the linked row's identifier value and `{{attributes.<name>}}` for a link attribute.

```json
{"formatPattern": "{{value}} ({{attributes.percentage}}%)"}
```

Pattern and definitions are validated against each other in **both** directions: a pattern referring to an attribute that does not exist is rejected, and removing an attribute that a pattern still refers to is rejected too. Since a pattern has to be removable before its definitions can be cleared, `formatPattern: null` is accepted and means "delete". So clearing everything in one request is:

```json
{"linkAttributes": [], "formatPattern": null}
```

Like `linkAttributes`, omitting `formatPattern` leaves it untouched.

### Writing values

#### The dedicated endpoint

```
PUT /tables/{tableId}/columns/{columnId}/rows/{rowId}/link/{linkId}/attributes
```

```json
{"attributes": [75]}
```

The response is the full cell value, as a cell `GET` would return it.

> **Careful with `{linkId}`.** In this path it is the **id of the linked (target) row**, not the `link_id` of the link relation. This matches the existing `DELETE …/link/{linkId}` and `PUT …/link/{linkId}/order`, so it is the convention rather than a one-off — but it is genuinely confusing, because `link_id` means something else everywhere in the database.

#### Inline on a cell write

A link entry may be an object instead of a bare id, in both accepted shapes:

```json
{"value": {"values": [{"id": 1, "attributes": [50]}]}}
{"value": {"to": 1, "attributes": [50]}}
```

Bare ids and objects can be mixed freely; an entry without `attributes` simply stores none:

```json
{"value": {"values": [1, {"id": 2, "attributes": [75]}]}}
```

```json
{
  "status": "ok",
  "value": [
    {"id": 1, "value": "table2row1"},
    {"id": 2, "value": "table2row2", "attributes": [75]}
  ]
}
```

#### Which verb actually updates an existing link

Inline `attributes` are written **only when the link itself is created**. The insert into the link table is guarded by `WHERE NOT EXISTS`, and that insert is the only place an inline value is bound. So for a link that already exists:

| Request | Result for `{"id": X, "attributes": [99]}` when the link to X already exists |
| --- | --- |
| `POST` / `PATCH` on the cell (appends links) | **500 `error.database.checkSize`**, whole transaction rolled back — even if the attributes are identical |
| `PUT` on the cell (replaces the cell) | attributes become `99`, but only because the link is deleted and re-created |
| `PUT …/link/{linkId}/attributes` | attributes become `99`, a real in-place update |

The 500 is pre-existing behaviour of the link insert guard (`insertCheckSize`), not something this feature introduced: appending a link that already exists has always failed that way. It is called out here because "just write the cell again" is the intuitive way to change an attribute, and it does not work.

**Use the dedicated endpoint to change the attributes of an existing link.**

### The positional contract, and `null`

`attributes[i]` belongs to `linkAttributes[i]`. A write must supply exactly one value per definition; a different length is rejected with `Expected N link attribute value(s) but got M.`

`null` is valid for every kind and means "cleared". It keeps its slot:

```json
{"value": {"values": [{"id": 1, "attributes": [null]}]}}
```

```json
{"status": "ok", "value": [{"id": 1, "value": "table2row1", "attributes": [null]}]}
```

That is different from having no `attributes` at all:

- nothing stored for the link → the `attributes` key is **absent** from the response
- a slot explicitly set to `null` → the key is **present**, with `null` in that slot

Sending `"attributes": []` on a column that has no definitions is accepted and stores nothing, so old clients that always send an empty array are unaffected.

### Normalisation

`date` values are normalised to `YYYY-MM-DD`, `datetime` values are converted to UTC and stored as `YYYY-MM-DDTHH:mm:ss.sssZ`. Writing `"2020-01-01T13:00:00.000+01:00"` gives you `"2020-01-01T12:00:00.000Z"` back.

### Changing a definition migrates existing values

Definitions are not versioned and values are not discarded. A change to the definition list migrates what is already stored:

| Change | What happens to stored values |
| --- | --- |
| `kind` changed | every value in that slot is cast to the new kind |
| `multilanguage` flipped | values in that slot are reshaped — wrapped into a langtag object, or collapsed back to a single value |
| definition added | every stored array is padded with `null` in the new slot |
| definition removed | every stored array is truncated |
| `name` or display info changed | **nothing** — a rename is cosmetic and keeps its values |

Two consequences worth internalising. First, the diff runs **by position, never by name**, which is exactly why a rename is free — see [ADR 0007](../adr/0007-link-attribute-values-are-positional.md). The flip side is that **reordering definitions is not free**: it reinterprets every stored value against its new slot.

Second, a value that cannot be cast fails the **entire** change, not just that row. This mirrors what happens when you change the kind of a normal column, where Postgres' `ALTER COLUMN … USING …::type` behaves the same way.

### Current rollout limits

Two restrictions apply today. Neither is structural — the code behind them works — they are waiting on the frontend:

- **At most one definition per link column.** More is rejected with `400 error.json.linkAttributes`.
- **`multilanguage: true` is rejected**, also with `400 error.json.linkAttributes`.

### `400` versus `422` on multilanguage

Error ids do not travel in the response body — see [Errors](../../GETTING_STARTED.md#23-errors) for where to read them.

Two multilanguage failures look alike and mean opposite things:

- **`400 error.json.linkAttributes`** — the rollout gate above. Temporary. It will disappear when the gate is lifted, and your request will start succeeding without you changing anything.
- **`422 unprocessable.entity`** — a permanent rule: neither of the two linked tables has any langtags, so a multilanguage attribute has no languages to hold. This will still fail after the gate is lifted.

Do not treat them as the same class of error. For everything else, the per-operation responses in `/docs/swagger.json` list what each endpoint can return.

### Backwards compatibility

A link column without link attributes behaves exactly as before. `linkAttributes` is omitted from the column response when the list is empty, `formatPattern` is omitted when unset, and no new key appears in the cell response. Every request shape that was valid for link values before is still valid.

---

## Part 2 — How it works

### Where things are stored

| What | Where | Scope |
| --- | --- | --- |
| Definitions | `system_link_table.attributes` (jsonb array) | one per **link relation** — both directions read the same row |
| Values | `link_table_<linkId>.attributes` (jsonb array) | one per **link**, positional |
| `formatPattern` | `system_columns.format_pattern` | one per **column** — the backlink column has its own, created as NULL |

That split is the whole design in one table: definitions are shared, patterns are not.

`schema_v43` adds `attributes` to `system_link_table` and, via a temporary plpgsql helper, to every existing `link_table_%`. New link tables get the column directly from the DDL in `ColumnModel.createLinkColumn`.

### Value migration

Three helpers in [`ColumnModel.scala`](../../src/main/scala/com/campudus/tableaux/database/model/structure/ColumnModel.scala) do the work, each skipped when the corresponding property did not change:

- **`castLinkAttributeValues`** — `jsonb_set` with the value cast to the new type; `date`/`datetime` go through `TO_CHAR` so the stored format matches exactly what the write path produces. (A test pins the write path and this path to each other, because one uses Joda and the other Postgres.)
- **`reshapeLinkAttributeValues`** — language-neutral to multilanguage wraps the value into an object for each langtag; the other direction collapses it, preferring the configured langtags in their configured order and skipping cleared ones.
- **`resizeLinkAttributeValues`** — pads with `'null'::jsonb` or truncates. Each variant carries a `WHERE` clause so a link table with millions of rows is not rewritten when nothing needs to change; clearing all definitions is a single `SET attributes = NULL`.

`updateLinkAttributesDefinition` orchestrates them, folding over `oldDefinitions.zip(newDefinitions).zipWithIndex` sequentially — they all rewrite the same jsonb column, so they cannot run in parallel. A failure anywhere rolls the whole column change back.

### The round-trip invariant

**Whatever the API hands out must be acceptable as a write.** Several decisions that look arbitrary in isolation exist to hold this:

- `null` is valid for every kind — otherwise a cleared value could be read but not written back.
- Langtag keys in values are **not** validated against the tables' langtags. A value belongs to the link, which is shared by two tables whose langtag sets may differ, and langtags can be removed after a value was written. Rejecting unknown keys would break read-modify-write and row duplication.
- Adding a definition pads existing arrays, so a value read before the change is still the right length after it.
- The read projection merges `attributes` in **outside** `jsonb_strip_nulls`. Otherwise a langtag deliberately set to `null` would silently vanish from the response and an all-null value would collapse to `{}`.

Breaking this invariant is not theoretical: it previously made `duplicateRow` fail with a 400 on its own output.

### Cache invalidation

`invalidateDependentColumnCaches` in [`StructureController.scala`](../../src/main/scala/com/campudus/tableaux/controller/StructureController.scala) runs after any structure change, `linkAttributes` included. It invalidates the dependent columns, each dependent table's column 0 (the concat column) and the group columns on both. Without it the backlink side, concat values and group columns kept serving stale cell values after a definition change.

### History

Attribute values appear in cell history entries alongside the link's `id` and `value`, and the dedicated endpoint writes history too. `retrieveLinkAttributesByRowId` short-circuits to an empty map when the column has no definitions, so link columns that predate this feature do not pay a guaranteed-empty round trip per column per row.

### Rollout gates and the test seam

The two limits live in `object LinkAttributeDefinition` in [`link.scala`](../../src/main/scala/com/campudus/tableaux/database/domain/link.scala):

| | Default | Read through | Test override |
| --- | --- | --- | --- |
| max definitions | `defaultMaxCount = 1` | `maxCount` | `setMaxCountForTest` / `resetMaxCountForTest` |
| multilanguage | `defaultMultilanguageSupported = false` | `multilanguageSupported` | `setMultilanguageSupportedForTest` / `resetMultilanguageSupportedForTest` |

They are `def`s over a `private var` override rather than constants, because the limits are rollout decisions and not structural ones. The N-definition and multilanguage paths are fully implemented — values are positional arrays, migrations walk them slot by slot, patterns address them by name — and hard-coding the limits would turn all of that into unreachable code no test could exercise. Over HTTP they behave exactly like constants.

They are not `TableauxConfig` entries because the checks happen in `JsonUtils` (request parsing) and `ColumnModel` (value migration), neither of which has a config in reach — and a config key would be an operator-facing switch for raising a limit the frontend cannot handle yet.

Tests opt in via the `LinkAttributeTestOverrides` trait, which applies both overrides in `@Before` and resets both in `@After`, so a class that lifts one gate is still held to the production default of the other. `LinkAttributeRolloutGatesTest` deliberately lifts nothing: it pins what the API answers *with* the gates in place.

#### Lifting a gate

1. Change the `default…` value in `link.scala`. Nothing else in production code should need to move.
2. Drop the corresponding `testLinkAttributeMaxCount` / `testMultilanguageLinkAttributesSupported` override from the test classes that raise it — `MultipleLinkAttributesTest` covers the N-definition paths, `LinkAttributesTest` and `ChangeLinkAttributesStructureTest` cover multilanguage values and definition changes. Those tests then run against the real default.
3. Delete the matching cases in `LinkAttributeRolloutGatesTest`; they assert a rejection that no longer happens. Keep `createLinkColumnWithLanguageNeutralAttributeSucceeds`.
4. Check what stops being a `400`: a request that used to fail with `error.json.linkAttributes` will now be accepted. The `422` for "no langtags on either table" stays.
5. Confirm the frontend actually handles it — several definitions per column need UI for ordering and for the fact that reordering migrates values; multilanguage needs the langtag editor wired to a value that is not a column.

---

## Known limitations

1. **Patterns are not cross-validated across directions.** Definitions are shared between a link column and its backlink column, but `formatPattern` is per column. Changing definitions from one side is validated only against *that* side's pattern; a pattern on the other side referring to a removed attribute is not caught.
2. **Langtag keys in values are not validated.** Any key is accepted. This is deliberate — see the round-trip invariant above — but it means a typo in a langtag is stored rather than rejected.
3. **An uncastable value during a kind change surfaces as a 500.** The Postgres cast error propagates as `error.database.*` and the whole column change is rolled back. It is a client error in substance but not in status code.
4. **The two rollout gates** described above: one definition per column, no multilanguage.
5. **Inline `attributes` only apply when the link is created.** Changing them on an existing link needs the dedicated endpoint; see the verb table in Part 1.
