%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(shovel_test_utils).

-include_lib("common_test/include/ct.hrl").
-export([set_param/3, set_param_nowait/3, await_shovel/2, await_shovel1/2,
         shovels_from_status/0, get_shovel_status/2,
         await/1, await/2, clear_param/2]).

make_uri(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~b",
                                               [Hostname, Port]))).
set_param(Config, Name, Value) ->
    set_param_nowait(Config, Name, Value),
    await_shovel(Config, Name).

set_param_nowait(Config, Name, Value) ->
    Uri = make_uri(Config),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set, [
        <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  Uri},
                                      {<<"dest-uri">>, [Uri]} |
                                      Value], none]).

await_shovel(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_shovel1, [Config, Name]).

await_shovel1(_Config, Name) ->
    await(fun () -> lists:member(Name, shovels_from_status()) end).

shovels_from_status() ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {running, _}, _} <- S].

get_shovel_status(Config, Name) ->
    S = rabbit_ct_broker_helpers:rpc(
          Config, 0, rabbit_shovel_status, lookup, [{<<"/">>, Name}]),
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
    rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name, <<"acting-user">>]).
