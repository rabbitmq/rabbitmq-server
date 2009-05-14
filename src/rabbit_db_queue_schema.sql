create table message (
       msg_id bytea PRIMARY KEY,
       msg bytea,
       ref_count integer NOT NULL
);
create index message_msg_id_index on message (msg_id);

create table sequence (
       queue bytea PRIMARY KEY,
       next_read integer NOT NULL,
       next_write integer NOT NULL
);
create index sequence_queue_index on sequence (queue);

create table ledger (
       queue bytea NOT NULL,
       seq_id integer NOT NULL,
       is_delivered boolean NOT NULL,
       msg_id bytea NOT NULL
);
create index ledger_queue_seq_id_index on ledger (queue, seq_id);

