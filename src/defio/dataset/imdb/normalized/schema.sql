-- Drop tables in reverse topological order to satisfy foreign key constraints
DROP TABLE IF EXISTS rating;
DROP TABLE IF EXISTS principal_character;
DROP TABLE IF EXISTS principal;
DROP TABLE IF EXISTS episode;
DROP TABLE IF EXISTS crew;
DROP TABLE IF EXISTS name_known_for_title;
DROP TABLE IF EXISTS name_profession;
DROP TABLE IF EXISTS name;
DROP TABLE IF EXISTS title_aka_attribute;
DROP TABLE IF EXISTS title_aka_type;
DROP TABLE IF EXISTS title_aka;
DROP TABLE IF EXISTS title_genre;
DROP TABLE IF EXISTS title;
DROP TABLE IF EXISTS principal_category;
DROP TABLE IF EXISTS crew_type;
DROP TABLE IF EXISTS title_alt_type;
DROP TABLE IF EXISTS title_type;
DROP TABLE IF EXISTS genre;

-- Redshift does not support unbounded varchar, so specify the exact max length

CREATE TABLE genre (
    id integer PRIMARY KEY,
    name character varying(11) UNIQUE NOT NULL
);

CREATE TABLE title_type (
    id integer PRIMARY KEY,
    name character varying(12) UNIQUE NOT NULL
);

CREATE TABLE title_alt_type (
    id integer PRIMARY KEY,
    name character varying(11) UNIQUE NOT NULL
);

CREATE TABLE crew_type (
    id integer PRIMARY KEY,
    name character varying(8) UNIQUE NOT NULL
);

CREATE TABLE principal_category (
    id integer PRIMARY KEY,
    name character varying(19) UNIQUE NOT NULL
);

CREATE TABLE title (
    id character varying(10) PRIMARY KEY,
    title_type_id integer NOT NULL REFERENCES title_type(id),
    primary_title character varying(419) NOT NULL,
    original_title character varying(419) NOT NULL,
    is_adult boolean NOT NULL,
    start_year integer,
    end_year integer,
    runtime_minutes integer
);

CREATE TABLE title_genre (
    title_id character varying(10) REFERENCES title(id),
    genre_id integer REFERENCES genre(id),
    CONSTRAINT title_genre_pkey PRIMARY KEY (title_id, genre_id)
);

CREATE TABLE title_aka (
    id integer PRIMARY KEY,
    title_id character varying(10) NOT NULL REFERENCES title(id),
    title character varying(831) NOT NULL,
    region character varying(4),
    language character varying(3),
    is_original_title boolean
);

CREATE TABLE title_aka_type (
    id integer PRIMARY KEY,
    title_aka_id integer NOT NULL REFERENCES title_aka(id),
    ta_type_id integer NOT NULL REFERENCES title_alt_type(id)
);

CREATE TABLE title_aka_attribute (
    id integer PRIMARY KEY,
    title_aka_id integer NOT NULL REFERENCES title_aka(id),
    ta_attribute character varying(48) NOT NULL
);

CREATE TABLE name (
    id character varying(10) PRIMARY KEY,
    primary_name character varying(105) NOT NULL,
    birth_year integer,
    death_year integer
);

CREATE TABLE name_profession (
    id integer PRIMARY KEY,
    name_id character varying(10) NOT NULL REFERENCES name(id),
    profession character varying(25) NOT NULL
);

CREATE TABLE name_known_for_title (
    id integer PRIMARY KEY,
    name_id character varying(10) NOT NULL REFERENCES name(id),
    title_id character varying(10) NOT NULL REFERENCES title(id)
);

CREATE TABLE crew (
    id integer PRIMARY KEY,
    title_id character varying(10) NOT NULL REFERENCES title(id),
    crew_type_id integer NOT NULL REFERENCES crew_type(id),
    name_id character varying(10) NOT NULL REFERENCES name(id)
);

CREATE TABLE episode (
    id integer PRIMARY KEY,
    title_id character varying(10) NOT NULL REFERENCES title(id),
    parent_title_id character varying(10) NOT NULL REFERENCES title(id),
    season_number integer,
    episode_number integer
);

CREATE TABLE principal (
    id integer PRIMARY KEY,
    title_id character varying(10) NOT NULL REFERENCES title(id),
    principal_category_id integer NOT NULL REFERENCES principal_category(id),
    name_id character varying(10),
    job character varying(286)
);

CREATE TABLE principal_character (
    id integer PRIMARY KEY,
    principal_id integer NOT NULL REFERENCES principal(id),
    p_character character varying(1304) NOT NULL
);

CREATE TABLE rating (
    id integer PRIMARY KEY,
    title_id character varying(10) NOT NULL REFERENCES title(id),
    average_rating real NOT NULL,
    num_votes integer NOT NULL
);
