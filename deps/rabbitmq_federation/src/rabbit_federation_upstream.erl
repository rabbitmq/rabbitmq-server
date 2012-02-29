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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_upstream).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([to_table/1, to_string/1, from_set/2]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_federation_util, [pget_bin/3]).

%%----------------------------------------------------------------------------

to_table(#upstream{params   = #amqp_params_network{host         = H,
                                                   port         = P,
                                                   ssl_options  = S,
                                                   virtual_host = V},
                   exchange = XNameBin}) ->
    PortPart = case P of
                   undefined -> [];
                   _         -> [{<<"port">>, long, P}]
               end,
    Protocol = case S of
                   none -> <<"amqp">>;
                   _    -> <<"amqps">>
               end,
    {table, [{<<"host">>,         longstr, list_to_binary(H)},
             {<<"virtual_host">>, longstr, V},
             {<<"exchange">>,     longstr, XNameBin},
             {<<"protocol">>,     longstr, Protocol}] ++
         PortPart}.

to_string(#upstream{params   = #amqp_params_network{host         = H,
                                                    port         = P,
                                                    virtual_host = V},
                    exchange = XNameBin}) ->
    PortPart = case P of
                   undefined -> <<>>;
                   _         -> print("~w:", [P])
               end,
    print("~s:~s~s:~s", [H, PortPart, V, XNameBin]).

print(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).

from_set(SetName, #resource{name         = DefaultXNameBin,
                            virtual_host = DefaultVHost}) ->
    {ok, Sets} = application:get_env(rabbitmq_federation, upstream_sets),
    case pget(binary_to_list(SetName), Sets) of
        undefined -> {error, set_not_found};
        Set       -> Results = [from_props(P, DefaultXNameBin, DefaultVHost) ||
                                   P <- Set],
                     case [E || E = {error, _} <- Results] of
                         []      -> {ok, Results};
                         [E | _] -> E
                     end
    end.

from_props(Upst, DefaultXNameBin, DefaultVHost) ->
    {ok, Connections} = application:get_env(rabbitmq_federation, connections),
    case pget(connection, Upst) of
        undefined -> {error, no_connection_name};
        ConnName  -> case pget(ConnName, Connections) of
                         undefined  -> {error, {no_connection, ConnName}};
                         Conn       -> from_props_connection(
                                         Upst, ConnName, Conn, DefaultXNameBin,
                                         DefaultVHost)
                     end
    end.

from_props_connection(Upst, ConnName, Conn, DefaultXNameBin, DefaultVHost) ->
    {ok, DefaultUser} = application:get_env(rabbit, default_user),
    {ok, DefaultPass} = application:get_env(rabbit, default_pass),
    case pget(host, Conn, none) of
        none -> {error, {no_host, ConnName}};
        Host -> Params = #amqp_params_network{
                  host         = Host,
                  port         = pget    (port,         Conn),
                  virtual_host = pget_bin(virtual_host, Conn, DefaultVHost),
                  username     = pget_bin(username,     Conn, DefaultUser),
                  password     = pget_bin(password,     Conn, DefaultPass)},
                XNameBin = pget_bin(exchange, Upst, DefaultXNameBin),
                #upstream{params          = set_extra_params(Params, Conn),
                          exchange        = XNameBin,
                          prefetch_count  = pget(prefetch_count,  Conn, none),
                          reconnect_delay = pget(reconnect_delay, Conn, 1),
                          max_hops        = pget(max_hops,        Upst, 1),
                          expires         = pget(expires,         Conn, none),
                          message_ttl     = pget(message_ttl,     Conn, none),
                          ha_policy       = pget(ha_policy,       Conn, none),
                          connection_name = ConnName}
    end.

set_extra_params(Params, Conn) ->
    lists:foldl(fun (F, ParamsIn) -> F(ParamsIn, Conn) end, Params,
                [fun set_ssl_options/2,
                 fun set_heartbeat/2,
                 fun set_mechanisms/2]).

%%----------------------------------------------------------------------------

set_ssl_options(Params, Conn) ->
    case pget(protocol, Conn, "amqp") of
        "amqp"  -> Params;
        "amqps" -> Params#amqp_params_network{
                     ssl_options = pget(ssl_options, Conn)}
    end.

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
