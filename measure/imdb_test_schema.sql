CREATE TABLE name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    imdb_index character varying(12),
    imdb_id integer,
    gender character varying(1),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);

CREATE TABLE title (
    id integer NOT NULL PRIMARY KEY,
    title text NOT NULL,
    imdb_index character varying(12),
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years character varying(49),
    md5sum character varying(32)
);

CREATE TABLE cast_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL REFERENCES name(id),
    movie_id integer NOT NULL REFERENCES title(id),
    person_role_id integer,
    note text,
    nr_order integer,
    role_id integer NOT NULL
);

CREATE TABLE keyword (
    id integer NOT NULL PRIMARY KEY,
    keyword text NOT NULL,
    phonetic_code character varying(5)
);

CREATE TABLE movie_keyword (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES title(id),
    keyword_id integer NOT NULL REFERENCES keyword(id)
);

