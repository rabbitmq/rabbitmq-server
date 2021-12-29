%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
