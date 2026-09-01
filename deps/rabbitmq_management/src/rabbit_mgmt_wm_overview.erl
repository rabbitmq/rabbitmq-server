%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_overview).

-export([init/2]).
-export([to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    Overview0 = [
                 {cluster_name,              rabbit_nodes:cluster_name()},
                 {crypto_lib_version,        rabbit_runtime:crypto_lib_version()}
                 ] ++ rabbit_mgmt_features:get_product_info() 
                   ++ rabbit_mgmt_features:get_settings(ReqData),
    try
        case rabbit_mgmt_util:disable_stats(ReqData) of
            false ->
                Range = rabbit_mgmt_util:range(ReqData),
                Overview =
                    case rabbit_mgmt_util:is_monitor(Tags) of
                        true ->
                            Overview0 ++
                                [{K, maybe_map(V)} ||
                                    {K,V} <- rabbit_mgmt_db:get_overview(Range)] ++
                                [{node,               node()},
                                 {listeners,          listeners()},
                                 {contexts,           web_contexts(ReqData)}];
                        _ ->
                            Overview0 ++
                                [{K, maybe_map(V)} ||
                                    {K, V} <- rabbit_mgmt_db:get_overview(User, Range)]
                    end,
                rabbit_mgmt_util:reply(Overview, ReqData, Context);
            true ->
                VHosts = case rabbit_mgmt_util:is_monitor(Tags) of
                             true -> rabbit_vhost:list_names();
                             _   -> rabbit_mgmt_util:list_visible_vhosts_names(User)
                         end,

                ObjectTotals = case rabbit_mgmt_util:is_monitor(Tags) of
                                   true ->
                                       [{queues, rabbit_amqqueue:count()},
                                        {exchanges, rabbit_exchange:count()},
                                        {connections, rabbit_connection_tracking:count()}];
                                   _   ->
                                       [{queues, length([Q || V <- VHosts, Q <- rabbit_amqqueue:list(V)])},
                                        {exchanges, length([X || V <- VHosts, X <- rabbit_exchange:list(V)])}]
                               end,
                Overview = Overview0 ++
                    [{node, node()},
                     {object_totals, ObjectTotals}],
                rabbit_mgmt_util:reply(Overview, ReqData, Context)
        end
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

listeners() ->
    rabbit_mgmt_util:sort_list(
      [rabbit_mgmt_format:listener(L)
       || L <- rabbit_networking:active_listeners()],
      ["protocol", "port", "node"] ).

maybe_map(L) when is_list(L) -> maps:from_list(L);
maybe_map(V)                 -> V.

%%--------------------------------------------------------------------

web_contexts(ReqData) ->
    rabbit_mgmt_util:sort_list(
      lists:append(
        [fmt_contexts(N) || N <- rabbit_mgmt_wm_nodes:all_nodes(ReqData)]),
      ["description", "port", "node"]).

fmt_contexts(Node) ->
    [fmt_context(Node, C) || C <- pget(contexts, Node, [])].

fmt_context(Node, C) ->
  rabbit_mgmt_format:web_context([{node, pget(name, Node)} | C]).
