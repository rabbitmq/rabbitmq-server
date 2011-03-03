%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc. All rights reserved.
%%

-module(mysql_helper).

-compile(export_all).  %% BUGBUG: For debug only, proper exports later...
-export([]).

-include("include/emysql.hrl").

%% TODO: Eventually have config setting support for changing database
%% parameters...
-define(RABBIT_DB_POOL_NAME, rabbit_mysql_pool).
-define(RABBIT_DB_POOL_SIZE, 1).
-define(RABBIT_DB_USERNAME, "rabbitmq").
-define(RABBIT_DB_PASSWORD, "password").
-define(RABBIT_DB_HOSTNAME, "localhost").
-define(RABBIT_DB_PORT, 3306).
-define(RABBIT_DB_DBNAME, "rabbit_mysql_queues").
-define(RABBIT_DB_ENCODING, utf8).

-define(WHEREAMI, case process_info(self(), current_function) of {_,
{M,F,A}} -> [M,F,A] end).
-define(LOGENTER, rabbit_log:info("Entering ~p:~p/~p...~n", ?WHEREAMI)).

%%--------------------------------------------------------------------
%% Verify a MySQL connection pool exists; create the pool if it doesn't.
%%--------------------------------------------------------------------
ensure_connection_pool() ->
    rabbit_log:info("Ensuring connection pool ~p exists~n",
                    [?RABBIT_DB_POOL_NAME]),
    try emysql:add_pool(?RABBIT_DB_POOL_NAME,
                        ?RABBIT_DB_POOL_SIZE,
                        ?RABBIT_DB_USERNAME,
                        ?RABBIT_DB_PASSWORD,
                        ?RABBIT_DB_HOSTNAME,
                        ?RABBIT_DB_PORT,
                        ?RABBIT_DB_DBNAME,
                        ?RABBIT_DB_ENCODING)
    catch
        exit:pool_already_exists -> ok
    end.


%% NOTE:  What the MySQL protocol actually supports in prepared statements
%%        seems a bit non-uniform.  For example, 'COMMIT' is in, but
%%        'START TRANSACTION' isn't.  Fortunately, most of the things that
%%        are parametrizable, and thus prone to injection attacks and the
%%        like do seem to be there.
prepare_mysql_statements() ->
    Statements = [{insert_q_stmt,<<"INSERT INTO q(queue_name, m) VALUES(?,?)">>},
                  {insert_p_stmt,
                   <<"INSERT INTO p(seq_id, queue_name, m) VALUES(?,?,?)">>},
                  {insert_n_stmt,
                   <<"INSERT INTO n(queue_name, next_seq_id) VALUES(?,?)">>},
                  {delete_q_stmt,<<"DELETE FROM q WHERE queue_name = ?">>},
                  {delete_p_stmt,<<"DELETE FROM p WHERE queue_name = ?">>},
                  {delete_n_stmt,<<"DELETE FROM n WHERE queue_name = ?">>},
                  {delete_non_persistent_msgs_stmt,
                   <<"DELETE FROM q WHERE queue_name = ? AND is_persistent = FALSE">>},
                  {read_n_stmt,  <<"SELECT * FROM n WHERE queue_name = ?">>},
                  {put_n_stmt,   <<"REPLACE INTO n(queue_name, next_seq_id) VALUES(?,?)">>},
                  {clear_p_stmt,
                   <<"DELETE FROM p WHERE queue_name = ?">>},
                  {clear_q_stmt,
                   <<"DELETE FROM q WHERE queue_name = ?">>},
                  {count_p_stmt,
                   <<"SELECT COUNT(*) FROM p WHERE queue_name = ?">>},
                  {count_q_stmt,
                   <<"SELECT COUNT(*) FROM q WHERE queue_name = ?">>},
                  {count_n_stmt,
                   <<"SELECT COUNT(*) FROM n WHERE queue_name = ?">>},
                  {write_msg_to_q_stmt,
                   <<"INSERT INTO q(queue_name, m, is_persistent) VALUES (?,?,?)">>},
                  {q_peek_stmt,
                  <<"SELECT * FROM q WHERE queue_name = ? ORDER BY id LIMIT 1">>},
                  {q_delete_stmt,
                  <<"DELETE FROM q WHERE queue_name = ? and id = ?">>} ],
    [ emysql:prepare(StmtAtom, StmtBody) || {StmtAtom, StmtBody} <- Statements ].

begin_mysql_transaction() ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   <<"START TRANSACTION">>).

commit_mysql_transaction() ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   <<"COMMIT">>).

delete_queue_data(QueueName) ->
    %% TODO:  Error checking...
    [ emysql:execute(?RABBIT_DB_POOL_NAME,
                     Stmt,
                     [QueueName]) || Stmt <- [delete_q_stmt,
                                              delete_p_stmt,
                                              delete_n_stmt] ],
    ok.

read_n_record(QueueName) ->
    %% BUGBUG:  Ugly.  We really should convert the result to Erlang records
    %%          here and isolate rabbit_mysql_queue from any direct touching
    %%          of the emysql library, but we need to split some records out
    %%          to an include file first...
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   read_n_stmt,
                   [QueueName]).

%% TODO:  Consistent error handling...
write_n_record(DbQueueName, NextSeqId) ->
    Result = emysql:execute(?RABBIT_DB_POOL_NAME,
                            put_n_stmt,
                            [DbQueueName, NextSeqId]),
    case Result of
        #ok_packet{}    -> ok;
        #error_packet{} -> rabbit_log:error("Failed REPLACE on n Table (~p,~p)",
                                            [DbQueueName, NextSeqId])
    end.

%% Delete non-persistent msgs after a restart.  Maybe used within a
%% MySQL transaction.
-spec delete_nonpersistent_msgs(string()) -> ok.

delete_nonpersistent_msgs(DbQueueName) ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   delete_non_persistent_msgs_stmt,
                   [DbQueueName]),
    ok.

%% Clear the pending acks table of pending acks associated with the
%% indicated queue.  May be called inside a MySQL transaction.
-spec clear_table(atom(), string()) -> ok.

clear_table(TableType, DbQueueName) ->
    case TableType of
        p -> emysql:execute(?RABBIT_DB_POOL_NAME,
                            clear_p_stmt,
                            [DbQueueName]);
        q -> emysql:execute(?RABBIT_DB_POOL_NAME,
                            clear_q_stmt,
                            [DbQueueName])
    end,
    ok.

-spec count_rows_for_queue(atom(), string()) -> non_neg_integer().

count_rows_for_queue(TableType, DbQueueName) ->
    QueryResult = case TableType of
                      p -> emysql:execute(?RABBIT_DB_POOL_NAME,
                                          count_p_stmt,
                                          [DbQueueName]);
                      q -> emysql:execute(?RABBIT_DB_POOL_NAME,
                                          count_q_stmt,
                                          [DbQueueName]);
                      n -> emysql:execute(?RABBIT_DB_POOL_NAME,
                                          count_n_stmt,
                                          [DbQueueName])
                  end,
    {result_packet, _,_,[[Val]],_} = QueryResult,
    Val.

write_message_to_q(DbQueueName, Msg, IsPersistent) ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   write_msg_to_q_stmt,
                   [DbQueueName, term_to_binary(Msg), IsPersistent]),
    ok.

%% BUGBUG:  Since the q table shadows is_persistent for convenience, will
%%          the pending acks table need to as well?
write_message_to_p(DbQueueName, SeqId, Msg) ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   insert_p_stmt,
                   [SeqId, DbQueueName, term_to_binary(Msg)]),
    ok.

q_peek(DbQueueName) ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   q_peek_stmt,
                   [DbQueueName]).

delete_from_q(DbQueueName, DbId) ->
    emysql:execute(?RABBIT_DB_POOL_NAME,
                   q_delete_stmt,
                   [DbQueueName, DbId]),
    ok.


%% This is only for convenience in REPL debugging.  Get rid of it later.
wake_up() ->
    ensure_connection_pool(),
    prepare_mysql_statements().
