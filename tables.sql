create table "entries" (id serial primary key not null, sourcequeue character varying, jsondata character varying, fulltext tsvector, created timestamp with time zone);
create index wowindex on entries using gist(fulltext);

