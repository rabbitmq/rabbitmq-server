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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_core_ff).

-export([list/0,
        quorum_queue_migration/1]).

list() ->
    #{quorum_queue => #{
        desc          => "Support queues of type `quorum`",
        stability     => experimental,
        migration_fun => {?MODULE, quorum_queue_migration}
       },

      empty_basic_get_metric => #{
        desc          => "Count AMQP `basic.get` on empty queues in stats",
        stability     => stable
       }
     }.

quorum_queue_migration(enable) ->
    Tables = [rabbit_queue,
              rabbit_durable_queue],
    rabbit_table:wait(Tables),
    Fields = amqqueue:fields(amqqueue_v2),
    migrate_to_amqqueue_with_type(Tables, Fields).

migrate_to_amqqueue_with_type([Table | Rest], Fields) ->
    Fun = fun(Queue) -> amqqueue:upgrade_to(amqqueue_v2, Queue) end,
    case mnesia:transform_table(Table, Fun, Fields) of
        {atomic, ok}      -> migrate_to_amqqueue_with_type(Rest, Fields);
        {aborted, Reason} -> {error, Reason}
    end;
migrate_to_amqqueue_with_type([], _) ->
    ok.
