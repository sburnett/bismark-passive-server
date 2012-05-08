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

CREATE FUNCTION insert_new_update
(text, text, bigint, integer, bytea, integer)
RETURNS VOID AS
$$
INSERT INTO updates
(node_id,
 anonymization_context,
 session_id,
 sequence_number,
 pickle,
 size)
VALUES ($1, $2, $3, $4, $5, $6);
UPDATE sessions SET pickle_size = pickle_size + $6
WHERE node_id = $1
AND anonymization_context = $2
AND session_id = $3;
INSERT INTO sessions
(node_id, anonymization_context, session_id, pickle_size)
SELECT $1, $2, $3, $6
WHERE NOT EXISTS
(SELECT 1 FROM sessions
 WHERE node_id = $1
 AND anonymization_context = $2
 AND session_id = $3);
$$
LANGUAGE sql;
