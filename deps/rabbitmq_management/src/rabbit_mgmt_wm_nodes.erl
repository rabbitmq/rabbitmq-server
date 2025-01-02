%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_nodes).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([all_nodes/1, all_nodes_raw/0]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    try
        rabbit_mgmt_util:reply_list(all_nodes(ReqData), ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

%%--------------------------------------------------------------------

all_nodes(ReqData) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_mgmt_db:augment_nodes(
              all_nodes_raw(), rabbit_mgmt_util:range_ceil(ReqData));
        true ->
            all_nodes_raw()
    end.

all_nodes_raw() ->
    S = rabbit_db_cluster:cli_cluster_status(),
    Nodes = proplists:get_value(nodes, S),
    Types = proplists:get_keys(Nodes),
    Running = proplists:get_value(running_nodes, S),
    [[{name, Node}, {type, Type}, {running, lists:member(Node, Running)}, {being_drained, rabbit_maintenance:is_being_drained_local_read(Node)}] ||
        Type <- Types, Node <- proplists:get_value(Type, Nodes)].
