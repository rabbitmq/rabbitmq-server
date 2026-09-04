%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_init).
-export([init/2]).
-export([content_types_provided/2, is_authorized/2, to_json/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

init(Req0, _State) ->
    Req1 = rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE),
    Req2 = rabbit_mgmt_headers:set_no_cache_headers(Req1, ?MODULE),
    {cowboy_rest, Req2, #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    try
        UserTags = (Context#context.user)#user.tags,
        Nodes = case rabbit_mgmt_util:is_monitor(UserTags) of
            true -> rabbit_mgmt_wm_nodes:all_nodes(ReqData);
            false -> null
        end,
        Payload = [
            {settings, settings(ReqData)},
            {vhosts,   vhosts(ReqData, Context)},
            {nodes,    Nodes}
        ],
        %% content_types_provided/2 advertises application/bert alongside
        %% application/json, the same way rabbit_mgmt_util:reply/3 does for
        %% every other endpoint.
        case maps:get(media_type, ReqData, undefined) of
            {<<"application">>, <<"bert">>, _} ->
                {term_to_binary(Payload), ReqData, Context};
            _ ->
                {rabbit_json:encode(
                   rabbit_mgmt_format:prepare_for_encoding(Payload)),
                 ReqData, Context}
        end
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

settings(ReqData) ->
    [
        {product_info,              rabbit_mgmt_features:get_product_info()},
        {cluster_name,              rabbit_nodes:cluster_name()}
    ] ++ rabbit_mgmt_features:get_settings(ReqData).

vhosts(ReqData, Context) ->
    %% Must be the same shape /vhosts returns, which is what the UI used to
    %% fetch: the virtual host selector reads .name off each entry. Note
    %% that rabbit_mgmt_util:list_visible_vhosts/1 returns names, not vhost
    %% records, so it is not enough on its own.
    VHosts = rabbit_queue_type:vhosts_with_dqt(
               rabbit_mgmt_wm_vhosts:augmented(ReqData, Context)),
    %% list_names/0 does an unordered ets:select, and /vhosts sorts by name.
    lists:sort(
      fun(A, B) -> maps:get(name, A, <<>>) =< maps:get(name, B, <<>>) end,
      VHosts).
