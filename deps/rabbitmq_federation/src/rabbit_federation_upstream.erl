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
    Sets = rabbit_runtime_parameters:value(
             federation, upstream_sets, [{"upstreams", []}]),
    case pget(SetName, Sets) of
        undefined -> {error, set_not_found};
        Set       -> Results = [from_props(P, DefaultXNameBin, DefaultVHost) ||
                                   P <- Set],
                     case [E || E = {error, _} <- Results] of
                         []      -> {ok, Results};
                         [E | _] -> E
                     end
    end.

from_props(Upst, DefaultXNameBin, DefaultVHost) ->
    Connections = rabbit_runtime_parameters:value(federation, connections, []),
    case bget(connection, Upst) of
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
    case bget(host, Conn, none) of
        none -> {error, {no_host, ConnName}};
        Host -> Params = #amqp_params_network{
                  host         = binary_to_list(Host),
                  port         = bget(port,         Conn),
                  virtual_host = bget(virtual_host, Conn, DefaultVHost),
                  username     = bget(username,     Conn, DefaultUser),
                  password     = bget(password,     Conn, DefaultPass)},
                XNameBin = bget(exchange, Upst, DefaultXNameBin),
                #upstream{params          = set_extra_params(Params, Conn),
                          exchange        = XNameBin,
                          prefetch_count  = bget(prefetch_count,  Conn, none),
                          reconnect_delay = bget(reconnect_delay, Conn, 1),
                          max_hops        = bget(max_hops,        Upst, 1),
                          expires         = bget(expires,         Conn, none),
                          message_ttl     = bget(message_ttl,     Conn, none),
                          ha_policy       = bget(ha_policy,       Conn, none),
                          connection_name = ConnName}
    end.

set_extra_params(Params, Conn) ->
    lists:foldl(fun (F, ParamsIn) -> F(ParamsIn, Conn) end, Params,
                [fun set_ssl_options/2,
                 fun set_heartbeat/2,
                 fun set_mechanisms/2]).

%%----------------------------------------------------------------------------

set_ssl_options(Params, Conn) ->
    case bget(protocol, Conn, "amqp") of
        "amqp"  -> Params;
        "amqps" -> Params#amqp_params_network{
                     ssl_options = bget(ssl_options, Conn)}
    end.

set_heartbeat(Params, Conn) ->
    case bget(heartbeat, Conn, none) of
        none -> Params;
        H    -> Params#amqp_params_network{heartbeat = H}
    end.

%% TODO it would be nice to support arbitrary mechanisms here.
set_mechanisms(Params, Conn) ->
    case bget(mechanism, Conn, default) of
        default    -> Params;
        'EXTERNAL' -> Params#amqp_params_network{
                        auth_mechanisms =
                            [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.

bget(K, L)        -> pget(a2b(K), L).
bget(K, L, D)     -> pget(a2b(K), L, D).

a2b(A) -> list_to_binary(atom_to_list(A)).
