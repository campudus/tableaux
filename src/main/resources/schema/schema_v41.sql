CREATE TABLE user_setting_schemas (
  key           VARCHAR(255) NOT NULL,
  schema        JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key)
);

CREATE TABLE user_settings_global (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id),
  FOREIGN KEY (key) REFERENCES user_setting_schemas (key) ON DELETE CASCADE
);

CREATE TABLE user_settings_table (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  table_id      BIGINT NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id, table_id),
  FOREIGN KEY (key) REFERENCES user_setting_schemas (key) ON DELETE CASCADE
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
  FOREIGN KEY (key) REFERENCES user_setting_schemas (key) ON DELETE CASCADE
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
    },
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
            "minItems": 4,
            "maxItems": 4,
            "items": [
              {
                "type": "string"
              },
              {
                "type": "string"
              },
              {
                "type": "string"
              },
              {
                "type": "string"
              }
            ]
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
  INSERT INTO user_setting_schemas
    (key, schema)
  VALUES
    -- global setting schemas
    ('filterReset', boolean_schema),
    ('columnsReset', boolean_schema),
    ('annotationReset', boolean_schema),
    ('sortingReset', boolean_schema),
    ('sortingDesc', boolean_schema),
    ('markdownEditor', string_schema),
    -- tableView setting schemas
    ('columnWidths', integer_record_schema),
    ('annotationHighlight', string_schema),
    ('visibleColumns', integer_array_schema),
    ('columnOrdering', id_index_array_schema),
    ('rowsFilter', filter_schema)
  ;
END $$ LANGUAGE plpgsql;
