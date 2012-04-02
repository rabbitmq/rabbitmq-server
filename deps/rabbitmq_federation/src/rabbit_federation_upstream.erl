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

from_set(<<"all">>, X) ->
    Connections = rabbit_runtime_parameters:list(<<"federation_connection">>),
    Set = [[{<<"connection">>, pget(key, C)}] || C <- Connections],
    from_set_contents(Set, X);

from_set(SetName, X) ->
    case rabbit_runtime_parameters:value(
           <<"federation_upstream_set">>, SetName) of
        undefined -> {error, set_not_found};
        Set       -> from_set_contents(Set, X)
    end.

from_set_contents(Set, #resource{name = DefaultXNameBin}) ->
    Results = [from_props(P, DefaultXNameBin) || P <- Set],
    case [E || E = {error, _} <- Results] of
        []      -> {ok, Results};
        [E | _] -> E
    end.

from_props(Upst, DefaultXNameBin) ->
    case bget(connection, Upst, []) of
        undefined -> {error, no_connection_name};
        ConnName  -> case rabbit_runtime_parameters:value(
                            <<"federation_connection">>, ConnName) of
                         not_found  -> {error, {no_connection, ConnName}};
                         Conn       -> from_props_connection(
                                         Upst, ConnName, Conn, DefaultXNameBin)
                     end
    end.

from_props_connection(U, ConnName, C, DefaultXNameBin) ->
    case bget(uri, U, C, none) of
        none -> {error, {no_uri, ConnName}};
        URI  -> {ok, Params} = amqp_uri:parse(binary_to_list(URI)),
                XNameBin = bget(exchange, U, C, DefaultXNameBin),
                #upstream{params          = set_extra_params(Params, U, C),
                          exchange        = XNameBin,
                          prefetch_count  = bget(prefetch_count,  U, C, none),
                          reconnect_delay = bget(reconnect_delay, U, C, 1),
                          max_hops        = bget(max_hops,        U, C, 1),
                          expires         = bget(expires,         U, C, none),
                          message_ttl     = bget(message_ttl,     U, C, none),
                          ha_policy       = bget(ha_policy,       U, C, none),
                          connection_name = ConnName}
    end.

set_extra_params(Params, U, C) ->
    lists:foldl(fun (F, ParamsIn) -> F(ParamsIn, U, C) end, Params,
                [fun set_ssl_options/3,
                 fun set_mechanisms/3]).

%%----------------------------------------------------------------------------

set_ssl_options(Params, U, C) ->
    case bget(protocol, U, C, "amqp") of
        "amqp"  -> Params;
        "amqps" -> Params#amqp_params_network{
                     ssl_options = bget(ssl_options, U, C)}
    end.

%% TODO it would be nice to support arbitrary mechanisms here.
set_mechanisms(Params, U, C) ->
    case bget(mechanism, U, C, default) of
        default    -> Params;
        'EXTERNAL' -> Params#amqp_params_network{
                        auth_mechanisms =
                            [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.

bget(K, L1, L2) -> bget(K, L1, L2, undefined).

bget(K0, L1, L2, D) ->
    K = a2b(K0),
    case pget(K, L1, undefined) of
        undefined -> pget(K, L2, D);
        Result    -> Result
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).
