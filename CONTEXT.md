# Tableaux

Tableaux is a REST service for storing data in tables that can link to each other. This glossary fixes the words used for the concepts that are easy to confuse — especially around links, where the same word means different things in the API, the database and everyday speech.

## Language

### Links

**Link**:
A single connection between one row and one row in another (or the same) table. This is what carries link attribute values.
_Avoid_: edge, connection, relation

**Link relation**:
The relationship behind a pair of link columns, identified by a `link_id`. It owns the link attribute definitions and backs one `link_table_<linkId>` table.
_Avoid_: using "link" for this — a link is the individual connection

**Link column**:
A column of kind `link` that exposes, for each of its rows, the links reaching out from that row.
_Avoid_: relation column, reference column

**Backlink column**:
The link column automatically created in the target table for the opposite direction of the same link relation.
_Avoid_: reverse column, inverse link, mirror column

### Attributes

**Link attribute**:
A named scalar carried by a link itself rather than by either of the two rows it connects.
_Avoid_: link property, edge attribute

**Link attribute definition**:
The declaration of one link attribute — its `name`, `kind`, `multilanguage` flag and display info. Lives on the link relation and is therefore identical on both link columns, where it appears as `linkAttributes`.
_Avoid_: link attribute column — a definition is deliberately not a column

**Link attribute value**:
The value of one link attribute on one link, exposed as `attributes` on that link. Bound by position: `attributes[i]` belongs to `linkAttributes[i]`.
_Avoid_: attribute value (unqualified)

**Column attributes**:
The free-form JSON object that can be stored on any column as `attributes`. Older than and unrelated to link attributes, despite the identical key name.
_Avoid_: attributes (unqualified) — always say which of the three you mean

### Column model

**Identifier column**:
A column flagged `identifier`, whose value represents its row wherever that row is shown as a link value.
_Avoid_: key column, primary column, display column

**Concat column**:
The virtual column with id 0 that a table gets when its identifier columns need to be presented as one value.
_Avoid_: identifier column, combined column

**Group column**:
A column that combines several other columns into one field for display purposes.
_Avoid_: composite column

**Format pattern**:
A display template on a column, given as `formatPattern`, with `{{...}}` placeholders that are filled from the column's own value or its parts.
_Avoid_: template, display pattern

**Langtag**:
An RFC 5646 language tag. A multilanguage value is an object keyed by langtag.
_Avoid_: locale, language code

### Process

**Rollout gate**:
A deliberately temporary restriction that limits a capability the backend already supports, until the frontend is ready for it. Implemented as a constant with a test-only override rather than as a configuration flag, so the code behind the gate stays reachable by tests.
_Avoid_: feature flag — a feature flag is operator-facing and configurable, a rollout gate is neither
