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

-module(rabbit_federation_db).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(DICT, orddict).

-export([get_active_suffix/3, set_active_suffix/3]).

%%----------------------------------------------------------------------------

get_active_suffix(X, Upstream, Default) ->
    case rabbit_exchange:lookup(X) of
        {ok, #exchange{scratch = undefined}} ->
            Default;
        {ok, #exchange{scratch = Dict}} ->
            case ?DICT:find(key(Upstream), Dict) of
                {ok, Suffix} -> Suffix;
                error        -> Default
            end;
        {error, not_found} ->
            Default
    end.

set_active_suffix(X = #resource{name = XName, virtual_host = VHost},
                  Upstream = #upstream{upstream_set = SetName}, Suffix) ->
    ok = rabbit_exchange:update_scratch(
           X, fun(Scratch) ->
                      OldDict = case Scratch of
                                    undefined -> ?DICT:new();
                                    _         -> Scratch
                                end,
                      %% We want to make sure old keys don't stay around, so
                      %% reconstruct the dict from scratch.
                      {ok, Upstreams} = rabbit_federation_upstream:from_set(
                                          SetName, XName, VHost),
                      Key = key(Upstream),
                      lists:foldl(
                        fun(U, D) ->
                                K = key(U),
                                case K of
                                    Key -> ?DICT:store(K, Suffix, D);
                                    _   -> case ?DICT:find(K, OldDict) of
                                               {ok, V} -> ?DICT:store(K, V, D);
                                               _       -> D
                                           end
                                end
                        end, ?DICT:new(), Upstreams)
              end).

key(#upstream{connection_name = ConnName, exchange = X}) -> {ConnName, X}.
