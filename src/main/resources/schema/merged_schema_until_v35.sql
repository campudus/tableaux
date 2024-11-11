

CREATE TABLE public.file (
    uuid uuid NOT NULL,
    idfolder bigint,
    tmp boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone
);

CREATE TABLE public.file_lang (
    uuid uuid NOT NULL,
    langtag character varying(50) NOT NULL,
    title character varying(255),
    description character varying(255),
    internal_name character varying(255),
    external_name character varying(255),
    mime_type character varying(255)
);

CREATE TABLE public.folder (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    description character varying(255) NOT NULL,
    idparent bigint,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone
);

CREATE SEQUENCE public.folder_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.folder_id_seq OWNED BY public.folder.id;

CREATE TABLE public.system_attachment (
    table_id bigint NOT NULL,
    column_id bigint NOT NULL,
    row_id bigint NOT NULL,
    attachment_uuid uuid NOT NULL,
    ordering bigint NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone
);

CREATE TABLE public.system_column_groups (
    table_id bigint NOT NULL,
    group_column_id bigint NOT NULL,
    grouped_column_id bigint NOT NULL
);

CREATE TABLE public.system_columns (
    table_id bigint NOT NULL,
    column_id bigint NOT NULL,
    column_type character varying(255) NOT NULL,
    user_column_name character varying(255) NOT NULL,
    ordering bigint NOT NULL,
    link_id bigint,
    multilanguage character varying(255),
    identifier boolean DEFAULT false,
    country_codes text[],
    format_pattern character varying(255),
    separator boolean DEFAULT false NOT NULL,
    attributes json DEFAULT '{}'::json NOT NULL,
    rules json DEFAULT '[]'::json NOT NULL,
    hidden boolean DEFAULT false NOT NULL,
    max_length integer,
    min_length integer
);

CREATE TABLE public.system_columns_lang (
    table_id bigint NOT NULL,
    column_id bigint NOT NULL,
    langtag character varying(50) NOT NULL,
    name character varying(255),
    description text
);

CREATE TABLE public.system_link_table (
    link_id bigint NOT NULL,
    table_id_1 bigint,
    table_id_2 bigint,
    cardinality_1 integer DEFAULT 0,
    cardinality_2 integer DEFAULT 0,
    delete_cascade boolean DEFAULT false,
    archive_cascade boolean DEFAULT false,
    final_cascade boolean DEFAULT false,
    CONSTRAINT system_link_table_cardinality_1_check CHECK ((cardinality_1 >= 0)),
    CONSTRAINT system_link_table_cardinality_2_check CHECK ((cardinality_2 >= 0))
);

CREATE SEQUENCE public.system_link_table_link_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.system_link_table_link_id_seq OWNED BY public.system_link_table.link_id;

CREATE TABLE public.system_services (
    id bigint NOT NULL,
    type character varying(50) NOT NULL,
    name character varying(255) NOT NULL,
    ordering bigint,
    displayname json,
    description json,
    active boolean DEFAULT true,
    config jsonb,
    scope jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone
);

CREATE SEQUENCE public.system_services_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.system_services_id_seq OWNED BY public.system_services.id;

CREATE TABLE public.system_settings (
    key character varying(255) NOT NULL,
    value text
);

CREATE TABLE public.system_table (
    table_id bigint NOT NULL,
    user_table_name character varying(255) NOT NULL,
    is_hidden boolean DEFAULT false,
    ordering bigint,
    langtags text[],
    type text,
    group_id bigint,
    attributes json DEFAULT '{}'::json NOT NULL
);

CREATE TABLE public.system_table_lang (
    table_id bigint NOT NULL,
    langtag character varying(50) NOT NULL,
    name character varying(255),
    description text
);

CREATE SEQUENCE public.system_table_table_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.system_table_table_id_seq OWNED BY public.system_table.table_id;

CREATE TABLE public.system_tablegroup (
    id bigint NOT NULL
);

CREATE SEQUENCE public.system_tablegroup_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.system_tablegroup_id_seq OWNED BY public.system_tablegroup.id;

CREATE TABLE public.system_tablegroup_lang (
    id bigint NOT NULL,
    langtag character varying(50) NOT NULL,
    name character varying(255),
    description text
);

ALTER TABLE ONLY public.folder ALTER COLUMN id SET DEFAULT nextval('public.folder_id_seq'::regclass);

ALTER TABLE ONLY public.system_link_table ALTER COLUMN link_id SET DEFAULT nextval('public.system_link_table_link_id_seq'::regclass);

ALTER TABLE ONLY public.system_services ALTER COLUMN id SET DEFAULT nextval('public.system_services_id_seq'::regclass);

ALTER TABLE ONLY public.system_table ALTER COLUMN table_id SET DEFAULT nextval('public.system_table_table_id_seq'::regclass);

ALTER TABLE ONLY public.system_table ALTER COLUMN ordering SET DEFAULT currval('public.system_table_table_id_seq'::regclass);

ALTER TABLE ONLY public.system_tablegroup ALTER COLUMN id SET DEFAULT nextval('public.system_tablegroup_id_seq'::regclass);


INSERT INTO public.system_settings (key, value) VALUES ('langtags', '["de-DE", "en-GB"]');

ALTER TABLE ONLY public.file_lang
    ADD CONSTRAINT file_lang_pkey PRIMARY KEY (uuid, langtag);

ALTER TABLE ONLY public.file
    ADD CONSTRAINT file_pkey PRIMARY KEY (uuid);

ALTER TABLE ONLY public.folder
    ADD CONSTRAINT folder_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.system_attachment
    ADD CONSTRAINT system_attachment_pkey PRIMARY KEY (table_id, column_id, row_id, attachment_uuid);

ALTER TABLE ONLY public.system_column_groups
    ADD CONSTRAINT system_column_groups_pkey PRIMARY KEY (table_id, group_column_id, grouped_column_id);

ALTER TABLE ONLY public.system_columns_lang
    ADD CONSTRAINT system_columns_lang_pkey PRIMARY KEY (table_id, column_id, langtag);

ALTER TABLE ONLY public.system_columns
    ADD CONSTRAINT system_columns_pkey PRIMARY KEY (table_id, column_id);

ALTER TABLE ONLY public.system_link_table
    ADD CONSTRAINT system_link_table_pkey PRIMARY KEY (link_id);

ALTER TABLE ONLY public.system_services
    ADD CONSTRAINT system_services_name_key UNIQUE (name);

ALTER TABLE ONLY public.system_services
    ADD CONSTRAINT system_services_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);

ALTER TABLE ONLY public.system_table_lang
    ADD CONSTRAINT system_table_lang_pkey PRIMARY KEY (table_id, langtag);

ALTER TABLE ONLY public.system_table
    ADD CONSTRAINT system_table_pkey PRIMARY KEY (table_id);

ALTER TABLE ONLY public.system_tablegroup_lang
    ADD CONSTRAINT system_tablegroup_lang_pkey PRIMARY KEY (id, langtag);

ALTER TABLE ONLY public.system_tablegroup
    ADD CONSTRAINT system_tablegroup_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX system_columns_name ON public.system_columns USING btree (table_id, user_column_name);

CREATE UNIQUE INDEX system_folder_name ON public.folder USING btree (name, idparent);

CREATE UNIQUE INDEX system_table_name ON public.system_table USING btree (user_table_name);

ALTER TABLE ONLY public.file
    ADD CONSTRAINT file_idfolder_fkey FOREIGN KEY (idfolder) REFERENCES public.folder(id);

ALTER TABLE ONLY public.file_lang
    ADD CONSTRAINT file_lang_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.file(uuid) ON DELETE CASCADE;

ALTER TABLE ONLY public.folder
    ADD CONSTRAINT folder_idparent_fkey FOREIGN KEY (idparent) REFERENCES public.folder(id);

ALTER TABLE ONLY public.system_attachment
    ADD CONSTRAINT system_attachment_attachment_uuid_fkey FOREIGN KEY (attachment_uuid) REFERENCES public.file(uuid) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_attachment
    ADD CONSTRAINT system_attachment_table_id_column_id_fkey FOREIGN KEY (table_id, column_id) REFERENCES public.system_columns(table_id, column_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_attachment
    ADD CONSTRAINT system_attachment_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_column_groups
    ADD CONSTRAINT system_column_groups_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_column_groups
    ADD CONSTRAINT system_column_groups_table_id_group_column_id_fkey FOREIGN KEY (table_id, group_column_id) REFERENCES public.system_columns(table_id, column_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_column_groups
    ADD CONSTRAINT system_column_groups_table_id_grouped_column_id_fkey FOREIGN KEY (table_id, grouped_column_id) REFERENCES public.system_columns(table_id, column_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_columns_lang
    ADD CONSTRAINT system_columns_lang_table_id_column_id_fkey FOREIGN KEY (table_id, column_id) REFERENCES public.system_columns(table_id, column_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_columns_lang
    ADD CONSTRAINT system_columns_lang_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_columns
    ADD CONSTRAINT system_columns_link_id_fkey FOREIGN KEY (link_id) REFERENCES public.system_link_table(link_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_columns
    ADD CONSTRAINT system_columns_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_link_table
    ADD CONSTRAINT system_link_table_table_id_1_fkey FOREIGN KEY (table_id_1) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_link_table
    ADD CONSTRAINT system_link_table_table_id_2_fkey FOREIGN KEY (table_id_2) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_table
    ADD CONSTRAINT system_table_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.system_tablegroup(id) ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE ONLY public.system_table_lang
    ADD CONSTRAINT system_table_lang_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.system_table(table_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.system_tablegroup_lang
    ADD CONSTRAINT system_tablegroup_lang_id_fkey FOREIGN KEY (id) REFERENCES public.system_tablegroup(id) ON DELETE CASCADE;
