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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_app).

-behaviour(application).
-export([start/2, stop/1]).

%% This does the same dummy supervisor thing as rabbit_mgmt_app - see the
%% comment there.
-behaviour(supervisor).
-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

start(_Type, _StartArgs) ->
    {ok, Xs} = application:get_env(exchanges),
    [declare_exchange(X) || X <- Xs],
    rabbit_federation_links:go_all(),
    supervisor:start_link({local,?MODULE},?MODULE,[]).

stop(_State) ->
    ok.

%%----------------------------------------------------------------------------

declare_exchange(Props) ->
    {ok, DefaultVHost} = application:get_env(rabbit, default_vhost),
    VHost = list_to_binary(
              pget(virtual_host, Props, binary_to_list(DefaultVHost))),
    Params = rabbit_federation_util:local_params(),
    Params1 = Params#amqp_params_direct{virtual_host = VHost},
    {ok, Conn} = amqp_connection:start(Params1),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange    = list_to_binary(pget(exchange, Props)),
        type        = <<"x-federation">>,
        durable     = pget(durable,     Props, true),
        auto_delete = pget(auto_delete, Props, false),
        internal    = pget(internal,    Props, false),
        arguments   =
            [{<<"upstreams">>, array,  [{table, to_table(U)} ||
                                           U <- pget(upstreams, Props)]},
             {<<"type">>,      longstr, list_to_binary(pget(type, Props))}]}),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    ok.

to_table(U) ->
    Args = [{host,         longstr},
            {protocol,     longstr},
            {port,         long},
            {virtual_host, longstr},
            {exchange,     longstr}],
    [Row || {K, T} <- Args, Row <- [to_table_row(U, K, T)], Row =/= none].

to_table_row(U, Key, Type) ->
    case {Type, proplists:get_value(Key, U)} of
        {_,    undefined} -> none;
        {long, Value}     -> {list_to_binary(atom_to_list(Key)), Type, Value};
        {_,    Value}     -> {list_to_binary(atom_to_list(Key)), Type,
                              list_to_binary(Value)}
    end.

pget(K, P, D) ->
    proplists:get_value(K, P, D).

pget(K, P) ->
    case proplists:get_value(K, P) of
        undefined -> exit({error, key_missing, K});
        V         -> V
    end.

%%----------------------------------------------------------------------------

init([]) -> {ok, {{one_for_one, 3, 10}, []}}.
