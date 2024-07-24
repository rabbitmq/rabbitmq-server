%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(shovel_test_utils).

-include_lib("common_test/include/ct.hrl").
-export([set_param/3, set_param/4, set_param/5, set_param_nowait/3,
         await_shovel/2, await_shovel/3, await_shovel/4, await_shovel1/3,
         shovels_from_status/0, shovels_from_status/1, 
         get_shovel_status/2, get_shovel_status/3,
         restart_shovel/2,
         await/1, await/2, clear_param/2, clear_param/3, make_uri/2]).

make_uri(Config, Node) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~b",
                                               [Hostname, Port]))).

set_param(Config, Name, Value) ->
    set_param_nowait(Config, 0, 0, Name, Value),
    await_shovel(Config, 0, Name).

set_param(Config, Node, Name, Value) ->
    set_param(Config, Node, Node, Name, Value).

set_param(Config, Node, QueueNode, Name, Value) ->
    set_param_nowait(Config, Node, QueueNode, Name, Value),
    await_shovel(Config, Node, Name).

set_param_nowait(Config, Name, Value) ->
    set_param_nowait(Config, 0, 0, Name, Value).

set_param_nowait(Config, Node, QueueNode, Name, Value) ->
    Uri = make_uri(Config, QueueNode),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_runtime_parameters, set, [
        <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  Uri},
                                      {<<"dest-uri">>, [Uri]} |
                                      Value], none]).

await_shovel(Config, Name) ->
    await_shovel(Config, 0, Name).

await_shovel(Config, Node, Name) ->
    await_shovel(Config, Node, Name, running).

await_shovel(Config, Node, Name, ExpectedState) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      ?MODULE, await_shovel1, [Config, Name, ExpectedState]).

await_shovel1(_Config, Name, ExpectedState) ->
    Ret = await(fun() -> 
                  Status = shovels_from_status(ExpectedState),
                  lists:member(Name, Status)
          end, 30_000), 
    Ret.

shovels_from_status() ->    
    shovels_from_status(running).

shovels_from_status(ExpectedState) ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {State, _}, _} <- S, State == ExpectedState].

get_shovel_status(Config, Name) ->
    get_shovel_status(Config, 0, Name).

get_shovel_status(Config, Node, Name) ->
    S = rabbit_ct_broker_helpers:rpc(
          Config, Node, rabbit_shovel_status, lookup, [{<<"/">>, Name}]),
    case S of
        not_found ->
            not_found;
        _ ->
            {Status, Info} = proplists:get_value(info, S),
            proplists:get_value(blocked_status, Info, Status)
    end.

await(Pred) ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(100),
                 await(Pred)
    end.

await(_Pred, Timeout) when Timeout =< 0 ->
    error(await_timeout);
await(Pred, Timeout) ->
    case Pred() of
        true  -> ok;
        Other when Timeout =< 100 ->
            error({await_timeout, Other});
        _ -> timer:sleep(100),
             await(Pred, Timeout - 100)
    end.

clear_param(Config, Name) ->
    clear_param(Config, 0, Name).

clear_param(Config, Node, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name, <<"acting-user">>]).

restart_shovel(Config, Name) ->
    restart_shovel(Config, 0, Name).

restart_shovel(Config, Node, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                        Node, rabbit_shovel_util, restart_shovel, [<<"/">>, Name]).