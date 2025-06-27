CREATE TABLE user_settings_global (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id)
);

CREATE TABLE user_settings_table (
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  table_id      BIGINT NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (key, user_id, table_id)
);

CREATE TABLE user_settings_filter (
  id            BIGSERIAL NOT NULL,
  key           VARCHAR(255) NOT NULL,
  user_id       VARCHAR(255) NOT NULL,
  name          VARCHAR(255) NOT NULL,
  value         JSONB,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP WITHOUT TIME ZONE,

  PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION public.trigger__set_updated_at__if_modified()
  RETURNS trigger
  LANGUAGE plpgsql
AS $function$
BEGIN
  IF ROW (NEW.*) IS DISTINCT FROM ROW (OLD.*) THEN
    NEW.updated_at = now();
    RETURN NEW;
  ELSE
    RETURN OLD;
  END IF;
END;
$function$
;

CREATE TRIGGER user_settings_global__trigger__updated_at
  BEFORE UPDATE ON public.user_settings_global
  FOR EACH ROW
  EXECUTE PROCEDURE public.trigger__set_updated_at__if_modified();

CREATE TRIGGER user_settings_table__trigger__updated_at
  BEFORE UPDATE ON public.user_settings_table
  FOR EACH ROW
  EXECUTE PROCEDURE public.trigger__set_updated_at__if_modified();

CREATE TRIGGER user_settings_filter__trigger__updated_at
  BEFORE UPDATE ON public.user_settings_filter
  FOR EACH ROW
  EXECUTE PROCEDURE public.trigger__set_updated_at__if_modified();