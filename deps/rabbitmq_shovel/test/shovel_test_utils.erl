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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(shovel_test_utils).

-export([set_param/3, set_param_nowait/3, await_shovel/2, await_shovel1/2,
         shovels_from_status/0, await/1, clear_param/2]).

set_param(Config, Name, Value) ->
    set_param_nowait(Config, Name, Value),
    await_shovel(Config, Name).

set_param_nowait(Config, Name, Value) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set, [
        <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  <<"amqp://">>},
                                      {<<"dest-uri">>, [<<"amqp://">>]} |
                                      Value], none]).

await_shovel(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_shovel1, [Config, Name]).

await_shovel1(_Config, Name) ->
    await(fun () -> lists:member(Name, shovels_from_status()) end).

shovels_from_status() ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {running, _}, _} <- S].

await(Pred) ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(100),
                 await(Pred)
    end.

clear_param(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name, <<"acting-user">>]).
