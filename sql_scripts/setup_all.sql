-- =============================================================================
-- Create database and set up rabbit user...
-- =============================================================================
CREATE DATABASE rabbit_mysql_queues;
CREATE USER 'rabbitmq'@'localhost' IDENTIFIED BY 'password';
GRANT ALL ON rabbit_mysql_queues.* TO 'rabbitmq'@'localhost';

USE rabbit_mysql_queues;

-- =============================================================================
-- Create Tables and Indices...
-- =============================================================================



-- Records in the 'q' table are messages, indexed by 'id', which corresponds
-- to the 'out_id' notion in rabbit_mnesia_queue.  We push new messages with
-- a new auto-incrementing id, and pop the message with the lowest id.  We
-- can't use the Rabbit seq_id for ordering since a given message will retain
-- its seq_id even if it gets requeued.
CREATE TABLE IF NOT EXISTS
             q (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                queue_name VARCHAR(256) NOT NULL,-- (BUGBUG: max size?)
                m MEDIUMBLOB NOT NULL, -- Max size L+3 bytes w/ L<2^(24)
                is_persistent BOOLEAN NOT NULL, -- Pushes work into MySQL
                PRIMARY KEY(id))
             ENGINE=InnoDB;
CREATE INDEX q_name_index ON q(queue_name);

-- A record in the 'n' table holders holds a pair of counters for a given
-- queue that represent the next seq_id and the next out_id from a queue
-- state, so that they can be recovered after a crash.  They are updated
-- on all MySQL transactions that update them in the in-RAM state.
CREATE TABLE IF NOT EXISTS
             n (queue_name VARCHAR(256) NOT NULL,
                next_seq_id BIGINT UNSIGNED NOT NULL, -- next seq_id to gen
                PRIMARY KEY(queue_name))
             ENGINE=InnoDB;


-- A record in the 'p' table is a pending ack.  It is indexed by the seq_id
-- and contains the message itself.  A given message is only in p or q,
-- the former if an ack is pending.
CREATE TABLE IF NOT EXISTS
             p (seq_id BIGINT UNSIGNED NOT NULL,
                queue_name VARCHAR(256) NOT NULL, -- (BUGBUG: max size?)
                m MEDIUMBLOB)
             ENGINE=InnoDB;
CREATE INDEX p_seq_id_index ON p(seq_id);
CREATE INDEX p_queue_name_index ON p(queue_name);

