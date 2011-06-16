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

-module(rabbit_federation_upstream).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([to_table/1, from_set_name/3]).

-import(rabbit_misc, [pget/2, pget/3]).

%%----------------------------------------------------------------------------

to_table(#upstream{params   = #amqp_params_network{host         = H,
                                                   port         = P,
                                                   virtual_host = V},
                   exchange = X}) ->
    {table, [{<<"host">>,         longstr, list_to_binary(H)},
             {<<"port">>,         long,    P},
             {<<"virtual_host">>, longstr, V},
             {<<"exchange">>,     longstr, X}]}.

from_set_name(SetName, DefaultXName, DefaultVHost) ->
    {ok, Sets} = application:get_env(rabbitmq_federation, upstream_sets),
    case pget(binary_to_list(SetName), Sets) of
        undefined -> {error, {"set not found", []}};
        Set       -> from_set(Set, DefaultXName, DefaultVHost)
    end.

from_set(Set, DefaultXName, DefaultVHost) ->
    Results = [from_props(P, DefaultXName, DefaultVHost) || P <- Set],
    case [E || E = {error, _} <- Results] of
        []      -> Results;
        [E | _] -> E
    end.

from_props(UpstreamProps, DefaultXName, DefaultVHost) ->
    {ok, Connections} = application:get_env(rabbitmq_federation, connections),
    case pget(connection, UpstreamProps) of
        undefined -> {error, {"no connection name", []}};
        ConnName  -> case pget(pget(connection, UpstreamProps), Connections) of
                         undefined  -> {error, {"connection ~s not found",
                                                [ConnName]}};
                         Conn       -> from_props_connection(
                                         UpstreamProps, ConnName, Conn,
                                         DefaultXName, DefaultVHost)
                     end
    end.

from_props_connection(UpstreamProps, ConnName, Conn,
                      DefaultXName, DefaultVHost) ->
    case pget(host, Conn, none) of
        none -> {error, {"no host in connection ~s", [ConnName]}};
        Host -> Params = #amqp_params_network{
                  host         = Host,
                  port         = pget(port,         Conn),
                  virtual_host = pget(virtual_host, Conn, DefaultVHost),
                  username     = pget(username,     Conn, <<"guest">>),
                  password     = pget(password,     Conn, <<"guest">>)},
                XName = pget(exchange, UpstreamProps, DefaultXName),
                #upstream{params          = set_extra_params(Params, Conn),
                          exchange        = list_to_binary(XName),
                          prefetch_count  = pget(prefetch_count,  Conn, none),
                          reconnect_delay = pget(reconnect_delay, Conn, 1),
                          expires         = pget(expires,         Conn, none),
                          message_ttl     = pget(message_ttl,     Conn, none)}
    end.

set_extra_params(Params, Conn) ->
    lists:foldl(fun (F, ParamsIn) -> F(ParamsIn, Conn) end, Params,
                [fun set_ssl_options/2,
                 fun set_default_port/2,
                 fun set_heartbeat/2,
                 fun set_mechanisms/2]).

%%----------------------------------------------------------------------------

set_ssl_options(Params, Conn) ->
    case pget(protocol, Conn, "amqp") of
        "amqp"  -> Params;
        "amqps" -> Params#amqp_params_network{
                     ssl_options = pget(ssl_options, Conn)}
    end.

%% TODO this should be part of the Erlang client - see bug 24138
set_default_port(Params = #amqp_params_network{port        = undefined,
                                               ssl_options = none}, _Conn) ->
    Params#amqp_params_network{port = 5672};
set_default_port(Params = #amqp_params_network{port = undefined}, _Conn) ->
    Params#amqp_params_network{port = 5671};
set_default_port(Params = #amqp_params_network{}, _Conn) ->
    Params.

set_heartbeat(Params, Conn) ->
    case pget(heartbeat, Conn, none) of
        none -> Params;
        H    -> Params#amqp_params_network{heartbeat = H}
    end.

%% TODO it would be nice to support arbitrary mechanisms here.
set_mechanisms(Params, Conn) ->
    case pget(mechanism, Conn, default) of
        default    -> Params;
        'EXTERNAL' -> Params#amqp_params_network{
                        auth_mechanisms =
                            [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.
