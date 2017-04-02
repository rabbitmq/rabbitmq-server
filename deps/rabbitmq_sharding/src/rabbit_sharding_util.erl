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
%% The Original Code is RabbitMQ Sharding Plugin
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_sharding_util).

-export([shard/1, sharded_exchanges/1]).
-export([get_policy/2, shards_per_node/1, routing_key/1]).
-export([exchange_bin/1, make_queue_name/3, a2b/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-import(rabbit_misc, [pget/3]).

shard(X) ->
    case get_policy(<<"shards-per-node">>, X) of
        undefined -> false;
        _SPN      -> true
    end.

sharded_exchanges(VHost) ->
    [X || X <- rabbit_exchange:list(VHost), shard(X)].

shards_per_node(X) ->
    get_policy(<<"shards-per-node">>, X).

routing_key(X) ->
    case get_policy(<<"routing-key">>, X) of
        undefined ->
            <<>>;
        Value ->
            Value
    end.

get_policy(Key, X) ->
    rabbit_policy:get(Key, X).

exchange_bin(#resource{name = XBin}) -> XBin.

make_queue_name(QBin, NodeBin, QNum) ->
    %% we do this to prevent unprintable characters in queue names
    QNumBin = list_to_binary(lists:flatten(io_lib:format("~p", [QNum]))),
    <<"sharding: ", QBin/binary, " - ", NodeBin/binary, " - ", QNumBin/binary>>.

a2b(A) -> list_to_binary(atom_to_list(A)).
