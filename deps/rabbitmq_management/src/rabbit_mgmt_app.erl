%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(CONTEXT, rabbit_mgmt).

start(_Type, _StartArgs) ->
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    {ok, _} = register_context(Listener, []),
    log_startup(Listener),
    rabbit_mgmt_sup_sup:start_link().

stop(_State) ->
    unregister_context(),
    ok.

%% At the point at which this is invoked we have both newly enabled
%% apps and about-to-disable apps running (so that
%% rabbit_mgmt_reset_handler can look at all of them to find
%% extensions). Therefore we have to explicitly exclude
%% about-to-disable apps from our new dispatcher.
reset_dispatcher(IgnoreApps) ->
    unregister_context(),
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    register_context(Listener, IgnoreApps).

register_context(Listener, IgnoreApps) ->
    rabbit_web_dispatch:register_context_handler(
      ?CONTEXT, Listener, "",
      rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
      "RabbitMQ Management").

unregister_context() ->
    rabbit_web_dispatch:unregister_context(?CONTEXT).

log_startup(Listener) ->
    rabbit_log:info("Management plugin started. Port: ~w~n", [port(Listener)]).

port(Listener) ->
    proplists:get_value(port, Listener).
