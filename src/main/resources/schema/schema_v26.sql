CREATE TABLE system_services (
  id          BIGSERIAL,
  type        VARCHAR(50) NOT NULL,
  name        VARCHAR(255) NOT NULL UNIQUE,
  ordering    BIGINT,
  displayname JSON,
  description JSON,
  active      BOOLEAN DEFAULT TRUE,
  config      JSONB,
  scope       JSONB,
  created_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (id)
);

