CREATE TABLE user_setting_kinds (
  kind          VARCHAR(255) NOT NULL,

  PRIMARY KEY (kind)
);

CREATE TABLE user_setting_schemas (
  name          VARCHAR(255) NOT NULL,
  schema        JSONB,

  PRIMARY KEY (name)
);

CREATE TABLE user_setting_keys (
  key           VARCHAR(255) NOT NULL,
  kind          VARCHAR(255) NOT NULL,
  schema        VARCHAR(255) NOT NULL,

  PRIMARY KEY (key),
  FOREIGN KEY (kind) REFERENCES user_setting_kinds (kind) ON DELETE CASCADE,
  FOREIGN KEY (schema) REFERENCES user_setting_schemas (name) ON DELETE CASCADE
);

CREATE TABLE user_settings_global (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id),
  FOREIGN KEY (key) REFERENCES user_setting_keys (key) ON DELETE CASCADE
);

CREATE TABLE user_settings_table (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  table_id      BIGINT NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id, table_id),
  FOREIGN KEY (key) REFERENCES user_setting_keys (key) ON DELETE CASCADE
);

CREATE TABLE user_settings_filter (
  id            BIGSERIAL NOT NULL,
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  name          VARCHAR(255) NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (id),
  FOREIGN KEY (key) REFERENCES user_setting_keys (key) ON DELETE CASCADE
);

DO $$
DECLARE
  boolean_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type":"object",
    "required": ["value"],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "boolean"
      }
    }
  }';
  string_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type":"object",
    "required": ["value"],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "string"
      }
    }
  }';
  integer_record_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type":"object",
    "required": ["value"],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "object",
        "patternProperties": {
          "^[0-9]+$": {
            "type": "integer"
          }
        }
      }
    }
  }';
  integer_array_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type":"object",
    "required": ["value"],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "array",
        "items": {
          "type": "integer"
        }
      }
    }
  }';
  id_index_array_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type":"object",
    "required": ["value"],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "id": { "type": "integer" },
            "idx": { "type": "integer" }
          },
          "required": ["id", "idx"],
          "additionalProperties": false
        }
      }
    }
  }';
  filter_schema jsonb := '{
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": [
      "value"
    ],
    "additionalProperties": false,
    "properties": {
      "value": {
        "type": "object",
        "properties": {
          "sortDirection": {
            "type": "string",
            "enum": [
              "asc",
              "desc"
            ]
          },
          "sortColumnName": {
            "type": "string"
          },
          "filters": {
            "$ref": "#/definitions/filter"
          }
        }
      }
    },
    "definitions": {
      "filter": {
        "anyOf": [
          {
            "type": "array",
            "minItems": 3,
            "maxItems": 4,
            "items": {
              "type": "string"
            }
          },
          {
            "type": "array",
            "minItems": 2,
            "items": [
              {
                "type": "string",
                "enum": [
                  "and",
                  "or"
                ]
              }
            ],
            "additionalItems": {
              "$ref": "#/definitions/filter"
            }
          }
        ]
      }
    }
  }';

BEGIN
  INSERT INTO user_setting_kinds
    (kind)
  VALUES
    ('global'),
    ('table'),
    ('filter')
  ;

  INSERT INTO user_setting_schemas
    (name, schema)
  VALUES
    ('string', string_schema),
    ('boolean', boolean_schema),
    ('integer_record', integer_record_schema),
    ('integer_array', integer_array_schema),
    ('id_index_array', id_index_array_schema),
    ('filter', filter_schema)
  ;

  INSERT INTO user_setting_keys
    (key, kind, schema)
  VALUES
    -- global setting schemas
    ('filterReset', 'global', 'boolean'),
    ('columnsReset', 'global', 'boolean'),
    ('annotationReset', 'global', 'boolean'),
    ('sortingReset', 'global', 'boolean'),
    ('sortingDesc', 'global', 'boolean'),
    ('markdownEditor', 'global', 'string'),
    -- table setting schemas
    ('columnWidths', 'table', 'integer_record'),
    ('annotationHighlight', 'table', 'string'),
    ('visibleColumns', 'table', 'integer_array'),
    ('columnOrdering', 'table', 'id_index_array'),
    ('rowsFilter', 'table', 'filter'),
    -- filter setting schema
    ('presetFilter', 'filter', 'filter')
  ;
END $$ LANGUAGE plpgsql;
