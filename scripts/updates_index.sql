CREATE TABLE tarnames (tarname text PRIMARY KEY);
CREATE RULE tarname_unique AS
ON INSERT TO tarnames
WHERE EXISTS (SELECT tarname FROM tarnames WHERE tarname = NEW.tarname)
DO INSTEAD NOTHING;

CREATE TABLE sessions
(
    node_id text,
    anonymization_context text,
    session_id bigint,
    pickle_size bigint
);

CREATE TABLE updates
(
    node_id text,
    anonymization_context text,
    session_id bigint,
    sequence_number integer,
    pickle bytea,
    size integer
);
CREATE INDEX updates_index ON updates
(node_id, anonymization_context, session_id, sequence_number);
