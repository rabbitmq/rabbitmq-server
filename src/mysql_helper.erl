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
-define(RABBIT_DB_PASSWORD, "passw0rd").
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
ensure_connection_pool(PoolAtom) ->
    rabbit_log:info("Ensuring connection pool ~p exists~n", [PoolAtom]),
    %% TODO:  Args 2 through 7 (or 8?) should be config options w/ defaults
    Result = try emysql:add_pool(PoolAtom,
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

verify_queue_table_exists(QueueName) ->
    %% TODO:  Graceful handling of #error_packet{} return case...
    #ok_packet{} = emysql:execute(?RABBIT_DB_POOL_NAME,
                                  create_queue_table_statement(QueueName)).

prepare_statements() ->
    %% TODO:  Prepare SQL statements here where they're preparable...
    ok.

create_queue_table_statement(TableName) ->
    %% TODO: This will also need to create appropriate indexes...
    "CREATE TABLE IF NOT EXISTS "
    "RMQ_" ++ TableName ++
        "(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        "guid VARCHAR(64) NOT NULL,"
        "MsgProps BLOB,"
        "is_persistent BOOL NOT NULL,"
        "delivered BOOL NOT NULL,"
        "acked BOOL NOT NULL,"
        "fq_queuename VARCHAR(256) NOT NULL,"
        "last_modified TIMESTAMP(8)) ENGINE InnoDB;".


