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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_db).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(DICT, orddict).

-export([get_active_suffix/3, set_active_suffix/3, prune_scratch/2]).

%%----------------------------------------------------------------------------

get_active_suffix(XName, Upstream, Default) ->
    case rabbit_exchange:lookup_scratch(XName, federation) of
        {ok, Dict} ->
            case ?DICT:find(key(Upstream), Dict) of
                {ok, Suffix} -> Suffix;
                error        -> Default
            end;
        {error, not_found} ->
            Default
    end.

set_active_suffix(XName, Upstream, Suffix) ->
    ok = rabbit_exchange:update_scratch(
           XName, federation,
           fun(D) -> ?DICT:store(key(Upstream), Suffix, ensure(D)) end).

prune_scratch(XName, Upstreams) ->
    ok = rabbit_exchange:update_scratch(
           XName, federation,
           fun(D) -> Keys = [key(U) || U <- Upstreams],
                     ?DICT:filter(
                        fun(K, _V) -> lists:member(K, Keys) end, ensure(D))
           end).

key(#upstream{name = UpstreamName, exchange_name = XNameBin}) ->
    {UpstreamName, XNameBin}.

ensure(undefined) -> ?DICT:new();
ensure(D)         -> D.
