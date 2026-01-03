%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_resource_counts).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2, variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found ->
            {true, ReqData, Context};
        VHost ->
            {rabbit_vhost:exists(VHost), ReqData, Context}
    end.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    VHost = rabbit_mgmt_util:vhost(ReqData),
    Counts = compute_resource_counts(VHost, User, Tags),
    rabbit_mgmt_util:reply(Counts, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

compute_resource_counts(not_found, User, Tags) ->
    VHosts = rabbit_mgmt_util:list_visible_vhosts_names(User),
    resource_counts(VHosts, Tags);

compute_resource_counts(VHost, User, Tags) ->
    VisibleVHosts = rabbit_mgmt_util:list_visible_vhosts_names(User),
    case lists:member(VHost, VisibleVHosts) of
        true -> resource_counts([VHost], Tags);
        false -> []
    end.

resource_counts(VHosts, Tags) ->
    IsMonitor = rabbit_mgmt_util:is_monitor(Tags),
    [
        {bindings, count_bindings(VHosts)},
        {channels, length(rabbit_channel_tracking:list())},
        {connections, rabbit_connection_tracking:count()},
        {exchanges, count_exchanges(VHosts, IsMonitor)},
        {nodes, rabbit_nodes:running_count()},
        {operator_policies, count_operator_policies(VHosts)},
        {policies, count_policies(VHosts)},
        {queues, count_queues(VHosts, IsMonitor)},
        {vhosts, length(VHosts)}
    ].

%%--------------------------------------------------------------------

count_exchanges(_VHosts, true) ->
    %% Monitor users get efficient global count
    rabbit_exchange:count();
count_exchanges(VHosts, false) ->
    %% Non-monitor users get filtered count
    length([X || V <- VHosts, X <- rabbit_exchange:list(V)]).

count_queues(_VHosts, true) ->
    rabbit_amqqueue:count();
count_queues(VHosts, false) ->
    length([Q || V <- VHosts, Q <- rabbit_amqqueue:list(V)]).

count_policies(VHosts) ->
    length(lists:append([rabbit_policy:list(V) || V <- VHosts])).

count_operator_policies(VHosts) ->
    length(lists:append([rabbit_policy:list_op(V) || V <- VHosts])).

count_bindings(VHosts) ->
    length(lists:append([rabbit_binding:list(V) || V <- VHosts])).
