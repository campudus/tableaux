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
