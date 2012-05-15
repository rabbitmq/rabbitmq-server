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

-export([to_table/1, to_string/1, from_set/2, from_set/3]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_federation_util, [name/1, vhost/1]).

%%----------------------------------------------------------------------------

to_table(#upstream{original_uri = URI,
                   params       = Params,
                   exchange = X}) ->
    {table, [{<<"uri">>,          longstr, URI},
             {<<"virtual_host">>, longstr, vhost(Params)},
             {<<"exchange">>,     longstr, name(X)}]}.

to_string(#upstream{original_uri = URI,
                    exchange     = #exchange{name = XName}}) ->
    print("~s on ~s", [rabbit_misc:rs(XName), URI]).

print(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).

from_set(SetName, X, ConnName) ->
    rabbit_federation_util:find_upstreams(ConnName, from_set(SetName, X)).

from_set(<<"all">>, X) ->
    Connections = rabbit_runtime_parameters:list(<<"federation_connection">>),
    Set = [[{<<"connection">>, pget(key, C)}] || C <- Connections],
    from_set_contents(Set, X);

from_set(SetName, X) ->
    case rabbit_runtime_parameters:value(
           <<"federation_upstream_set">>, SetName) of
        not_found -> [];
        Set       -> from_set_contents(Set, X)
    end.

from_set_contents(Set, X) ->
    Results = [from_props(P, X) || P <- Set],
    [R || R <- Results, R =/= not_found].

from_props(Upst, X) ->
    ConnName = bget(connection, Upst, []),
    case rabbit_runtime_parameters:value(
           <<"federation_connection">>, ConnName) of
        not_found  -> not_found;
        Conn       -> from_props_connection(Upst, ConnName, Conn, X)
    end.

from_props_connection(U, ConnName, C, X) ->
    URI = bget(uri, U, C),
    {ok, Params} = amqp_uri:parse(binary_to_list(URI), vhost(X)),
    XNameBin = bget(exchange, U, C, name(X)),
    #upstream{params          = Params,
              original_uri    = URI,
              exchange        = with_name(XNameBin, vhost(Params), X),
              prefetch_count  = bget(prefetch_count,  U, C, ?DEFAULT_PREFETCH),
              reconnect_delay = bget(reconnect_delay, U, C, 1),
              max_hops        = bget(max_hops,        U, C, 1),
              expires         = bget(expires,         U, C, none),
              message_ttl     = bget(message_ttl,     U, C, none),
              ha_policy       = bget(ha_policy,       U, C, none),
              connection_name = ConnName}.

%%----------------------------------------------------------------------------

bget(K, L1, L2) -> bget(K, L1, L2, undefined).

bget(K0, L1, L2, D) ->
    K = a2b(K0),
    case pget(K, L1, undefined) of
        undefined -> pget(K, L2, D);
        Result    -> Result
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).

with_name(XNameBin, VHostBin, X) ->
    X#exchange{name = rabbit_misc:r(VHostBin, exchange, XNameBin)}.
